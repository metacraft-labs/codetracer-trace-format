// Instantiate the demo module and write out the `.ct` container it builds.
//
//   node run.mjs <module.wasm> <out.ct> [steps]
//
// Deliberately plain: `WebAssembly.instantiate` with an empty import object,
// no wasm-bindgen, no glue. The same handful of lines work in a browser —
// swap the `writeFileSync` for a `Blob` and a download link.

import { readFileSync, writeFileSync } from "node:fs";

const [modulePath, outPath, stepsArg] = process.argv.slice(2);
if (!modulePath || !outPath) {
  console.error("usage: node run.mjs <module.wasm> <out.ct> [steps]");
  process.exit(2);
}
const steps = Number(stepsArg ?? 64);

const bytes = readFileSync(modulePath);
const { instance } = await WebAssembly.instantiate(bytes, {});
const { ct_demo_build, ct_demo_len, memory } = instance.exports;

const imports = WebAssembly.Module.imports(new WebAssembly.Module(bytes));
console.log(`module: ${bytes.length} bytes, ${imports.length} import(s)`);
for (const i of imports) console.log(`  import ${i.module}.${i.name} (${i.kind})`);

const ptr = ct_demo_build(steps);
if (ptr === 0) {
  console.error("ct_demo_build returned null: the writer failed inside wasm");
  process.exit(1);
}
const len = ct_demo_len();
const container = new Uint8Array(memory.buffer, ptr, len).slice();

const magic = [...container.slice(0, 5)].map((b) => b.toString(16).padStart(2, "0")).join(" ");
console.log(`container: ${len} bytes, magic ${magic}, version ${container[5]}`);

writeFileSync(outPath, container);
console.log(`wrote ${outPath}`);
