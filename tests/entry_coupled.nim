## Required real coupled writer/reader artifact producer; no mocks or legacy-absence skip.
import std/[os, options]
import results
import codetracer_trace_writer/[multi_stream_writer, new_trace_reader]
proc saveImage(name: string, data: seq[byte]) =
  var s = newString(data.len)
  for i,b in data: s[i] = char(b)
  writeFile(name, s)
proc makeImage(marked: bool): seq[byte] =
  var w = initMultiStreamWriter("", "sealed-real-entry").get()
  doAssert w.registerPath("/src/program.sol").isOk
  doAssert w.registerFunctionAt("/src/program.sol", 1, "<toplevel>").get() == 0
  doAssert w.registerFunctionAt("/src/program.sol", 2, "compute").get() == 1
  doAssert w.registerCall(0, @[]).isOk
  doAssert w.registerStep(0, 1, @[]).isOk
  doAssert w.registerCall(1, @[]).isOk
  if marked: doAssert w.markCurrentCallAsEntry().isOk
  doAssert w.registerStep(0, 2, @[]).isOk
  doAssert w.registerReturn().isOk
  doAssert w.registerReturn().isOk
  doAssert w.close().isOk
  result = w.toBytes()

if paramStr(1) == "produce":
  saveImage(paramStr(2) / "marked.ct", makeImage(true))
  saveImage(paramStr(2) / "unmarked.ct", makeImage(false))
elif paramStr(1) == "read-rust":
  var marked = openNewTrace(paramStr(2) / "rust-marked.ct").get()
  let identity = marked.recordedEntryIdentity()
  doAssert identity.isSome
  doAssert identity.get().callKey == 1
  doAssert identity.get().functionId == 1
  doAssert identity.get().entryStep == 1
  var unmarked = openNewTrace(paramStr(2) / "rust-unmarked.ct").get()
  doAssert unmarked.recordedEntryIdentity().isNone
else:
  raise newException(ValueError, "Unknown required coupled entry operation")
