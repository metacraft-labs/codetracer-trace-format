//! The `NSB1` namespace image: a page-structured B-tree from 64-bit keys to
//! fixed-width descriptors, the form `linehits.tc` and `corrmark.ns` take.
//!
//! # Image
//!
//! The image is a whole number of 4096-byte pages. Page 0 is the header:
//!
//! ```text
//! [0..4)    magic "NSB1"
//! [4..12)   root_block[0]  u64 LE     [12..20)  root_block[1]  u64 LE
//! [20..28)  commit_id[0]   u64 LE     [28..36)  commit_id[1]   u64 LE
//! [36]      flags u8: bit 0 leaf type (0 = 8-byte descriptors, 1 = 16-byte),
//!                     bit 1 skip_sub_blocks
//! [37..45)  free_list_head u64 LE     [45..53)  next_free_page u64 LE
//! [53..61)  page_count     u64 LE     the rest of the page is zero
//! ```
//!
//! The committed root is the slot with the higher commit id (`0` in both:
//! empty). Pages `1..page_count` are nodes; any bytes after them are a payload
//! region that leaf descriptors address. A node page is
//!
//! ```text
//! [0] kind (0 internal, 1 leaf)  [1] 0  [2..4) count u16 LE  [4..8) 0
//! leaf:     count keys (u64 LE), then count descriptors
//! internal: count keys (u64 LE), then count + 1 child page numbers (u64 LE)
//! ```
//!
//! A lookup descends to the child after a separator equal to the key, so the
//! keys under child `i` are at least separator `i - 1` and below separator `i`.
//!
//! # Building
//!
//! [`bulk_load`] builds the tree bottom-up from sorted, unique keys: leaves of
//! `(4096 - 8) / (8 + descriptor width)` keys each, then internal levels of
//! that many plus one children, each separator the minimum key of the child
//! after it, pages numbered in the order they are filled (leaves left to
//! right, then each level), one commit (slot 0, id 1). [`payload_namespace`]
//! is the form both members use: 16-byte `[offset u64][length u64]`
//! descriptors into payloads appended after the node pages, in key order,
//! and the image padded with zeros to a whole page.

const PAGE: usize = 4096;
const NODE_HEADER: usize = 8;
const KIND_INTERNAL: u8 = 0;
const KIND_LEAF: u8 = 1;
const OFF_ROOT0: usize = 4;
const OFF_ROOT1: usize = 12;
const OFF_COMMIT0: usize = 20;
const OFF_COMMIT1: usize = 28;
const OFF_FLAGS: usize = 36;
const OFF_NEXT_FREE: usize = 45;
const OFF_PAGE_COUNT: usize = 53;

/// The width of a leaf's descriptors, declared by flags bit 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeafType {
    /// 8-byte descriptors.
    A,
    /// 16-byte descriptors.
    B,
}

impl LeafType {
    fn descriptor_size(self) -> usize {
        match self {
            LeafType::A => 8,
            LeafType::B => 16,
        }
    }

    fn bit(self) -> u8 {
        match self {
            LeafType::A => 0,
            LeafType::B => 1,
        }
    }

    fn order(self) -> usize {
        (PAGE - NODE_HEADER) / (8 + self.descriptor_size())
    }
}

fn put_u64(buf: &mut [u8], at: usize, v: u64) {
    buf[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

fn get_u64(buf: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(buf[at..at + 8].try_into().expect("eight bytes"))
}

fn get_u16(buf: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([buf[at], buf[at + 1]])
}

/// Build the image of a tree holding `entries`, which must be strictly
/// ascending by key and each carry a descriptor of the leaf type's width.
/// An empty `entries` gives a one-page image with no committed root.
pub fn bulk_load(leaf: LeafType, skip_sub_blocks: bool, entries: &[(u64, Vec<u8>)]) -> Result<Vec<u8>, String> {
    let width = leaf.descriptor_size();
    for (i, (key, desc)) in entries.iter().enumerate() {
        if desc.len() != width {
            return Err(format!("descriptor size mismatch at entry {i}: got {} expected {width}", desc.len()));
        }
        if i > 0 && *key <= entries[i - 1].0 {
            return Err(format!("bulkLoad batch not strictly ascending at entry {i}"));
        }
    }
    let order = leaf.order();
    let mut pages = vec![0u8; PAGE];
    let mut next_page: u64 = 1;
    let mut alloc = |pages: &mut Vec<u8>| -> (u64, usize) {
        let page = next_page;
        next_page += 1;
        pages.resize(pages.len() + PAGE, 0);
        (page, page as usize * PAGE)
    };
    let mut root = 0u64;
    if !entries.is_empty() {
        let mut level: Vec<(u64, u64)> = Vec::new();
        for run in entries.chunks(order) {
            let (page, base) = alloc(&mut pages);
            pages[base] = KIND_LEAF;
            pages[base + 2..base + 4].copy_from_slice(&(run.len() as u16).to_le_bytes());
            for (i, (key, _)) in run.iter().enumerate() {
                put_u64(&mut pages, base + NODE_HEADER + i * 8, *key);
            }
            let descs = base + NODE_HEADER + run.len() * 8;
            for (i, (_, desc)) in run.iter().enumerate() {
                pages[descs + i * width..descs + (i + 1) * width].copy_from_slice(desc);
            }
            level.push((page, run[0].0));
        }
        while level.len() > 1 {
            let mut parent = Vec::new();
            for group in level.chunks(order + 1) {
                let (page, base) = alloc(&mut pages);
                let keys = group.len() - 1;
                pages[base] = KIND_INTERNAL;
                pages[base + 2..base + 4].copy_from_slice(&(keys as u16).to_le_bytes());
                for (i, (_, min_key)) in group.iter().skip(1).enumerate() {
                    put_u64(&mut pages, base + NODE_HEADER + i * 8, *min_key);
                }
                let children = base + NODE_HEADER + keys * 8;
                for (i, (child, _)) in group.iter().enumerate() {
                    put_u64(&mut pages, children + i * 8, *child);
                }
                parent.push((page, group[0].1));
            }
            level = parent;
        }
        root = level[0].0;
    }
    pages[0..4].copy_from_slice(b"NSB1");
    put_u64(&mut pages, OFF_ROOT0, root);
    put_u64(&mut pages, OFF_COMMIT0, if root == 0 { 0 } else { 1 });
    pages[OFF_FLAGS] = leaf.bit() | if skip_sub_blocks { 0b10 } else { 0 };
    put_u64(&mut pages, OFF_NEXT_FREE, next_page);
    put_u64(&mut pages, OFF_PAGE_COUNT, next_page);
    Ok(pages)
}

fn descriptor(offset: u64, len: u64) -> Vec<u8> {
    let mut d = Vec::with_capacity(16);
    d.extend_from_slice(&offset.to_le_bytes());
    d.extend_from_slice(&len.to_le_bytes());
    d
}

/// The image of a Type-B tree (sub-blocks skipped) whose descriptors address
/// `payloads`, appended after the node pages in key order, the image padded
/// with zeros to a whole page. `payloads` must be strictly ascending by key.
pub fn payload_namespace(payloads: &[(u64, &[u8])]) -> Result<Vec<u8>, String> {
    let sizing: Vec<(u64, Vec<u8>)> = payloads.iter().map(|(k, _)| (*k, descriptor(0, 0))).collect();
    let base = bulk_load(LeafType::B, true, &sizing)?.len() as u64;
    let mut offset = base;
    let mut entries = Vec::with_capacity(payloads.len());
    for (key, data) in payloads {
        entries.push((*key, descriptor(offset, data.len() as u64)));
        offset += data.len() as u64;
    }
    let mut image = bulk_load(LeafType::B, true, &entries)?;
    for (_, data) in payloads {
        image.extend_from_slice(data);
    }
    image.resize(image.len().div_ceil(PAGE) * PAGE, 0);
    Ok(image)
}

/// An opened, checked namespace image.
#[derive(Debug, Clone)]
pub struct CowNamespace {
    image: Vec<u8>,
    leaf: LeafType,
    root: u64,
    keys: u64,
}

impl CowNamespace {
    /// Open `image`, refusing one a lookup could not trust: shorter than a
    /// page or not a whole number of pages; without the `NSB1` magic; with
    /// flag bits other than 0 and 1 or a leaf type other than `leaf`; with a
    /// `page_count` of 0 or more pages than the image holds; whose committed
    /// slot (the higher non-zero commit id) names root 0; or whose committed
    /// tree reaches a page outside `[1, page_count)`, reaches a page
    /// twice, has a node of unknown kind, with no key, with more keys than a
    /// page holds or with non-zero reserved header bytes, has keys out of
    /// order or outside the range the parent's separators give them, or has
    /// leaves at different depths.
    pub fn open(image: Vec<u8>, leaf: LeafType) -> Result<Self, String> {
        if image.len() < PAGE {
            return Err(format!("namespace B-tree image is {} bytes, shorter than its header page", image.len()));
        }
        if &image[0..4] != b"NSB1" {
            return Err("invalid namespace B-tree magic".to_string());
        }
        if !image.len().is_multiple_of(PAGE) {
            return Err("image not page-aligned".to_string());
        }
        let flags = image[OFF_FLAGS];
        if flags & !0b11 != 0 {
            return Err(format!("namespace B-tree: unknown flag bits 0x{flags:02X}"));
        }
        if flags & 1 != leaf.bit() {
            return Err(format!("namespace B-tree: leaf type {} where {} is expected", flags & 1, leaf.bit()));
        }
        let page_count = get_u64(&image, OFF_PAGE_COUNT);
        let image_pages = (image.len() / PAGE) as u64;
        if page_count == 0 || page_count > image_pages {
            return Err(format!(
                "namespace B-tree: page_count {page_count} does not fit the {image_pages}-page image"
            ));
        }
        let (c0, c1) = (get_u64(&image, OFF_COMMIT0), get_u64(&image, OFF_COMMIT1));
        let root = if c0 == 0 && c1 == 0 {
            0
        } else if c1 > c0 {
            get_u64(&image, OFF_ROOT1)
        } else {
            get_u64(&image, OFF_ROOT0)
        };
        if root == 0 && (c0 != 0 || c1 != 0) {
            return Err(format!("namespace B-tree: commit id {} names no root", c0.max(c1)));
        }
        let mut ns = CowNamespace { image, leaf, root, keys: 0 };
        ns.keys = ns.validate(page_count)?;
        Ok(ns)
    }

    fn validate(&self, page_limit: u64) -> Result<u64, String> {
        struct Frame {
            page: u64,
            depth: usize,
            lo: Option<u64>,
            hi: Option<u64>,
        }
        if self.root == 0 {
            return Ok(0);
        }
        let width = self.leaf.descriptor_size();
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![Frame {
            page: self.root,
            depth: 0,
            lo: None,
            hi: None,
        }];
        let mut leaf_depth: Option<usize> = None;
        let mut keys = 0u64;
        while let Some(f) = stack.pop() {
            if f.page == 0 || f.page >= page_limit {
                return Err(format!("namespace B-tree: page {} is outside the tree's {page_limit} pages", f.page));
            }
            if !visited.insert(f.page) {
                return Err(format!("namespace B-tree: page {} is reached twice", f.page));
            }
            let base = f.page as usize * PAGE;
            let node = &self.image[base..base + PAGE];
            if node[1] != 0 || node[4..8].iter().any(|&b| b != 0) {
                return Err(format!("namespace B-tree: page {} has non-zero reserved header bytes", f.page));
            }
            let count = get_u16(node, 2) as usize;
            if count == 0 {
                return Err(format!("namespace B-tree: page {} holds no keys", f.page));
            }
            let is_leaf = match node[0] {
                KIND_LEAF => true,
                KIND_INTERNAL => false,
                kind => return Err(format!("namespace B-tree: page {} has node kind {kind}", f.page)),
            };
            let needed = if is_leaf {
                NODE_HEADER + count * (8 + width)
            } else {
                NODE_HEADER + count * 8 + (count + 1) * 8
            };
            if needed > PAGE {
                return Err(format!("namespace B-tree: page {} claims {count} keys, more than a page holds", f.page));
            }
            for i in 0..count {
                let k = get_u64(node, NODE_HEADER + i * 8);
                if i > 0 && k <= get_u64(node, NODE_HEADER + (i - 1) * 8) {
                    return Err(format!("namespace B-tree: keys of page {} do not ascend", f.page));
                }
                if f.lo.is_some_and(|lo| k < lo) || f.hi.is_some_and(|hi| k >= hi) {
                    return Err(format!(
                        "namespace B-tree: key {k} of page {} is outside the range its parent gives it",
                        f.page
                    ));
                }
            }
            if is_leaf {
                match leaf_depth {
                    None => leaf_depth = Some(f.depth),
                    Some(d) if d != f.depth => {
                        return Err(format!("namespace B-tree: leaves at depths {d} and {}", f.depth));
                    }
                    Some(_) => {}
                }
                keys += count as u64;
            } else {
                let children = NODE_HEADER + count * 8;
                for i in (0..=count).rev() {
                    stack.push(Frame {
                        page: get_u64(node, children + i * 8),
                        depth: f.depth + 1,
                        lo: if i == 0 { f.lo } else { Some(get_u64(node, NODE_HEADER + (i - 1) * 8)) },
                        hi: if i == count { f.hi } else { Some(get_u64(node, NODE_HEADER + i * 8)) },
                    });
                }
            }
        }
        Ok(keys)
    }

    /// The whole image: node pages and the payload region after them.
    pub fn image(&self) -> &[u8] {
        &self.image
    }

    /// Number of keys in the committed tree.
    pub fn len(&self) -> u64 {
        self.keys
    }

    pub fn is_empty(&self) -> bool {
        self.keys == 0
    }

    fn node(&self, page: u64) -> (&[u8], usize) {
        let base = page as usize * PAGE;
        let node = &self.image[base..base + PAGE];
        (node, get_u16(node, 2) as usize)
    }

    /// The descriptor stored under `key`, or `None` when the tree has no such
    /// key.
    pub fn lookup(&self, key: u64) -> Option<&[u8]> {
        let width = self.leaf.descriptor_size();
        let mut page = self.root;
        if page == 0 {
            return None;
        }
        loop {
            let (node, count) = self.node(page);
            let keys = |i: usize| get_u64(node, NODE_HEADER + i * 8);
            let (mut lo, mut hi) = (0, count);
            while lo < hi {
                let mid = (lo + hi) / 2;
                if keys(mid) < key {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            let found = lo < count && keys(lo) == key;
            if node[0] == KIND_LEAF {
                if !found {
                    return None;
                }
                let at = NODE_HEADER + count * 8 + lo * width;
                let base = page as usize * PAGE;
                return Some(&self.image[base + at..base + at + width]);
            }
            let child = if found { lo + 1 } else { lo };
            page = get_u64(node, NODE_HEADER + count * 8 + child * 8);
        }
    }

    /// Every key, ascending.
    pub fn keys(&self) -> Vec<u64> {
        let mut out = Vec::with_capacity(self.keys as usize);
        if self.root != 0 {
            self.collect(self.root, &mut out);
        }
        out
    }

    fn collect(&self, page: u64, out: &mut Vec<u64>) {
        let (node, count) = self.node(page);
        if node[0] == KIND_LEAF {
            out.extend((0..count).map(|i| get_u64(node, NODE_HEADER + i * 8)));
        } else {
            for i in 0..=count {
                self.collect(get_u64(node, NODE_HEADER + count * 8 + i * 8), out);
            }
        }
    }

    /// The payload a 16-byte `[offset u64][length u64]` descriptor
    /// addresses, refused when it does not lie inside the image.
    pub fn payload(&self, descriptor: &[u8]) -> Result<&[u8], String> {
        if descriptor.len() != 16 {
            return Err(format!("descriptor is {} bytes, not 16", descriptor.len()));
        }
        let off = get_u64(descriptor, 0);
        let len = get_u64(descriptor, 8);
        let image_len = self.image.len() as u64;
        if off > image_len || len > image_len - off {
            return Err("payload out of bounds".to_string());
        }
        Ok(&self.image[off as usize..(off + len) as usize])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn desc(i: u64) -> Vec<u8> {
        descriptor(i, 0)
    }

    #[test]
    fn a_bulk_loaded_tree_answers_every_key_and_no_other() {
        for n in [0u64, 1, 170, 171, 400, 29_000, 30_000] {
            let entries: Vec<(u64, Vec<u8>)> = (0..n).map(|k| (k * 3, desc(k))).collect();
            let ns = CowNamespace::open(bulk_load(LeafType::B, true, &entries).unwrap(), LeafType::B).unwrap();
            assert_eq!(ns.len(), n);
            assert_eq!(ns.keys(), entries.iter().map(|e| e.0).collect::<Vec<_>>());
            for (k, d) in &entries {
                assert_eq!(ns.lookup(*k), Some(d.as_slice()), "n={n} key {k}");
                assert_eq!(ns.lookup(k + 1), None);
            }
        }
    }

    #[test]
    fn unsorted_or_duplicate_keys_are_refused() {
        assert!(bulk_load(LeafType::B, true, &[(2, desc(0)), (1, desc(0))]).is_err());
        assert!(bulk_load(LeafType::B, true, &[(1, desc(0)), (1, desc(0))]).is_err());
        assert!(bulk_load(LeafType::A, true, &[(1, desc(0))]).is_err());
    }
}
