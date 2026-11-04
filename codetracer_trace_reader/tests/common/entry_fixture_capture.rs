//! Capture exact authentic producer bytes as one complete committed generation.
//! Private copies retain the original file-reader assertions and trace bytes.
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};
use tempfile::TempDir;
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    schema: String,
    images: Images,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Images {
    #[serde(rename = "marked.ct")]
    marked: String,
    #[serde(rename = "unmarked.ct")]
    unmarked: String,
}
#[cfg(unix)]
type Physical = (u64, u64, u32);
#[cfg(windows)]
type Physical = (u32, u32, u32, u32);
#[cfg(unix)]
fn physical(file: &File) -> Physical {
    use std::os::unix::fs::MetadataExt;
    let m = file.metadata().unwrap();
    (m.dev(), m.ino(), m.mode())
}
#[cfg(windows)]
fn physical(file: &File) -> Physical {
    use std::os::windows::io::AsRawHandle;
    #[repr(C)]
    struct Info {
        attributes: u32,
        creation: [u32; 2],
        access: [u32; 2],
        write: [u32; 2],
        volume: u32,
        size_high: u32,
        size_low: u32,
        links: u32,
        index_high: u32,
        index_low: u32,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFileInformationByHandle(handle: *mut std::ffi::c_void, info: *mut Info) -> i32;
    }
    let mut info = std::mem::MaybeUninit::<Info>::uninit();
    assert_eq!(std::mem::size_of::<Info>(), 52);
    assert_ne!(
        unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) },
        0,
        "native image identity query"
    );
    let i = unsafe { info.assume_init() };
    assert_eq!(i.attributes & 0x400, 0, "reparse image refused");
    (i.volume, i.index_high, i.index_low, i.attributes)
}
fn open_image(path: &Path, directory: bool) -> File {
    let lexical = fs::symlink_metadata(path).expect("required producer authority");
    assert!(!lexical.file_type().is_symlink());
    assert_eq!(lexical.is_dir(), directory);
    assert!(directory || lexical.is_file());
    let mut options = File::options();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | if directory { libc::O_DIRECTORY } else { 0 });
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
        assert_eq!(lexical.file_attributes() & 0x400, 0);
        options.share_mode(if directory { 3 } else { 1 });
        options.custom_flags(0x00200000 | if directory { 0x02000000 } else { 0 });
        if directory {
            options.access_mode(0x80);
        }
    }
    let file = options.open(path).expect("held producer image");
    let held = file.metadata().unwrap();
    assert_eq!(held.is_dir(), directory);
    assert!(directory || held.is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!((lexical.dev(), lexical.ino(), lexical.mode()), physical(&file));
    }
    #[cfg(windows)]
    {
        let i = physical(&file);
        assert_eq!(i.3 & 0x10 != 0, directory);
    }
    file
}
struct Held {
    path: PathBuf,
    resolved: PathBuf,
    file: File,
    physical: Physical,
    directory: bool,
}
impl Held {
    fn open(path: &Path, directory: bool) -> Self {
        let resolved = fs::canonicalize(path).unwrap();
        let file = open_image(path, directory);
        let physical = physical(&file);
        let image = Self {
            path: path.to_owned(),
            resolved,
            file,
            physical,
            directory,
        };
        image.check();
        image
    }
    fn check(&self) {
        assert_eq!(fs::canonicalize(&self.path).unwrap(), self.resolved, "changed lexical/resolved image");
        assert_eq!(physical(&self.file), self.physical, "changed held image");
        let selected = open_image(&self.path, self.directory);
        assert_eq!(physical(&selected), self.physical, "lexical path changed physical authority");
    }
    fn bytes(&mut self) -> Vec<u8> {
        self.check();
        let mut b = Vec::new();
        self.file.read_to_end(&mut b).unwrap();
        self.check();
        assert_eq!(b.len() as u64, self.file.metadata().unwrap().len());
        b
    }
}
fn membership(root: &Path) -> Vec<std::ffi::OsString> {
    let mut names: Vec<_> = fs::read_dir(root).unwrap().map(|r| r.unwrap().file_name()).collect();
    names.sort();
    names
}
pub fn capture() -> TempDir {
    let root = PathBuf::from(std::env::var("ENTRY_NIM_FIXTURE_ROOT").expect("genuine Nim fixture root is required"));
    let mut parents = Vec::new();
    for parent in [root.parent().unwrap(), root.as_path()] {
        parents.push(Held::open(parent, true));
    }
    let expected: Vec<_> = [".producer-owner", "marked.ct", "unmarked.ct"]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();
    assert!(!root.join(".publisher-lock").exists(), "uncommitted generation");
    assert_eq!(membership(&root), expected);
    let mut owner = Held::open(&root.join(".producer-owner"), false);
    let owner_bytes = owner.bytes();
    let marker: Marker = serde_json::from_slice(&owner_bytes).expect("unique complete generation marker");
    assert_eq!(marker.schema, "codetracer-trace-format.entry-fixtures.v1");
    let captured = TempDir::new().unwrap();
    let mut images = Vec::new();
    for (name, encoded) in [("marked.ct", marker.images.marked), ("unmarked.ct", marker.images.unmarked)] {
        assert_eq!(encoded.len() % 2, 0);
        let expected: Vec<u8> = encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
            .collect();
        let mut image = Held::open(&root.join(name), false);
        let actual = image.bytes();
        assert_eq!(actual, expected, "changed genuine producer generation");
        fs::write(captured.path().join(name), &actual).unwrap();
        images.push(image);
    }
    for image in &images {
        image.check();
    }
    owner.check();
    assert_eq!(fs::read(&owner.path).unwrap(), owner_bytes, "generation changed during capture");
    assert!(!root.join(".publisher-lock").exists(), "concurrent producer mutation");
    assert_eq!(membership(&root), expected);
    for parent in &mut parents {
        parent.check();
    }
    captured
}
