"""Report only audited public failure fields; never diagnostic bodies."""
import json
from pathlib import Path
import re
import sys

MESSAGES = ('Actual installed package inventory schema differs',
 'Actual locked Nimble seed constructor is absent or ambiguous',
 'Actual locked peer loaded-RC association differs',
 'Actual locked runtime authority incomplete',
 'Actual metadata writer changed during execution',
 'Actual metadata writer inventory differs from independently held complete seed',
 'Actual native local hook differs from constructor',
 'Actual native-hook Python constructor absent',
 'Actual native-hook config argument absent',
 'Actual native-hook config constructor absent or ambiguous',
 'Actual native-hook config constructor role differs',
 'Actual native-hook config parser absent or ambiguous',
 'Actual native-hook transaction script absent or ambiguous',
 'Actual role constructor receipt changed at publication',
 'Actual role receipt write refused',
 'Actual selected Nimble data schema differs',
 'Actual selected release data schema differs',
 'Actual selected role derivation inventory differs',
 'Actual selected role derivation output differs',
 'Actual selected role outputs are missing',
 'Actual selected writer output role differs',
 'Actual supplier differs from owning generated lock',
 'Actual writer derivation inventory differs',
 'Actual writer recursive registered closure incomplete',
 'CI runtime requires the envrc pinned published plugin',
 'CI runtime requires the exact locked inputs, not sibling overrides',
 'Cached runtime evidence owner or kind differs',
 'Changed peer index',
 'Changed receipt authority',
 'Changed tracked execution parity',
 'Changed tracked link kind',
 'Changed tracked member bytes',
 'Darwin process ABI differs',
 'Darwin process census truncated or malformed',
 'Darwin process census unavailable',
 'Darwin process identity malformed',
 'Darwin process state unknown',
 'Declared Nimble source cache selector differs from owned seed output',
 'Declared activation Nix role differs from selected constructor',
 'Declared metadata writer store output schema differs',
 'Declared peer runtime operation refused',
 'Declared reader cache constructor output incomplete',
 'Declared reader changed another execution member',
 'Declared reader changed metadata physical ownership',
 'Duplicate runtime owner field',
 'Existing runtime authority incomplete',
 'Existing runtime output has no owned manifest',
 'Existing runtime output ownership correspondence refused',
 'Existing runtime owner selected images differ',
 'Existing runtime owner source membership differs',
 'Foreign CI flake override selector',
 'Foreign existing runtime output refused',
 'Foreign immutable Direnv runtime constructor',
 'Foreign locked Nim peer authority',
 'Foreign locked runtime supplier',
 'Foreign or nonregular reader/compiler output',
 'Foreign peer envrc',
 'Foreign peer namespace',
 'Foreign peer revision',
 'Foreign runtime output ancestor',
 'Foreign source-owned Direnv configuration',
 'Genuine metadata constructor output ownership differs',
 'Genuine metadata writer result differs',
 'Genuine reader output absent or nonexecutable',
 'Held cached runtime evidence changed',
 'Held runtime directory identity changed',
 'Held runtime executable identity changed',
 'Held runtime receipt identity changed',
 'Independently constructed locked runtime role absent',
 'Invalid actual source archive schema',
 'Memory-only supplier configuration changed',
 'Missing or duplicate locked Nim peer',
 'Native hook census differs from tracked constructor',
 'Native hook constructor changed undeclared Git configuration',
 'Native hook directory ownership differs',
 'Native hook held directory differs',
 'Native hook source is not an ordinary file',
 'Native-hook generated config link differs from actual constructor',
 'Native-hook transaction differs from the tracked peer constructor',
 'Nonregular selected image',
 'Nonunique locked runtime supplier',
 'Occupied expected runtime profile output',
 'Original supplier index changed',
 'Original supplier physical namespace changed',
 'Plugin differs from the actual envrc pinned body',
 'Preexisting CI generated runtime root refused',
 'Preexisting foreign native hook configuration refused',
 'Preexisting reader/compiler outputs refused',
 'Read-only reader dependency constructor changed the peer',
 'Reader dependency metadata differs from actual selected Nimble constructor',
 'Runtime constructor child still active',
 'Runtime constructor source changed',
 'Runtime image changed at hold',
 'Runtime tool is not a regular image',
 'Selected Nimble source constructor changed',
 'Selected Nix path-info schema differs',
 'Selected runtime Nix constructor version differs',
 'Selected runtime closure path set malformed',
 'Selected runtime image changed',
 'Selected runtime output differs from actual constructor',
 'Selected runtime recursive closure changed',
 'Selected runtime recursive closure incomplete',
 'Selected runtime role has no actual registered deriver',
 'Selected runtime role is outside declared store closure',
 'Selected source Git template absent',
 'Special peer namespace member',
 'Undeclared CI dotenv activation input',
 'Undeclared runtime output root',
 'Unknown authorization session census',
 'Unknown runtime constructor session census',
 'Unsupported actual Git config entry schema',
 'Unsupported actual Git config inventory schema',
 'Unsupported actual native hook role parser',
 'Unsupported actual native-hook config parser',
 'Unsupported actual print-dev-env PATH schema',
 'Unsupported actual seed package member',
 'Unsupported actual shellHook constructor schema',
 'Unsupported local hook config output ownership',
 'Unsupported native process identity constructor',
 'Unsupported primary native hook common directory',
 'Unsupported reader reverse-dependency schema',
 'Unsupported runtime source member',
 'Unsupported selected seed package constructor',
 'Unsupported tracked member',
 'Writer source/output differs from actual held constructor')
STAGES = ('activate-exact-locked-peer',
 'actual-locked-peer-cache-constructor',
 'actual-locked-peer-runtime-role-constructor',
 'actual-locked-peer-source-constructor',
 'actual-metadata-selected-nimble-source',
 'actual-metadata-writer-derivation',
 'actual-metadata-writer-derivation-attributes',
 'actual-metadata-writer-registered-closure',
 'actual-nimble-metadata-constructor',
 'actual-nimble-metadata-runtime-content',
 'actual-reader-dependency-constructor',
 'actual-source-declared-reader-build',
 'authorize-exact-tracked-envrc',
 'checkout-locked-peer',
 'clone-locked-peer',
 'genuine-nimble-two-metadata-writers',
 'prepare-real-runtime',
 'selected-runtime-content',
 'selected-runtime-nix-version',
 'selected-runtime-path-info',
 'source-config',
 'source-head',
 'verify-actual-generated-native-hooks',
 'verify-initial-native-hook-authority')
TYPES = {"RuntimeError", "FileNotFoundError", "PermissionError", "ValueError", "KeyError", "TypeError", "OSError", "JSONDecodeError"}

def summarize(value, receipt):
    result = {"receipt": receipt, "available": True}
    if not isinstance(value, dict):
        result["schemaAccepted"] = False
        return result
    for key in ("finished", "success", "finalGuards"):
        if type(value.get(key)) is bool:
            result[key] = value[key]
    failure_type = value.get("failureType")
    if isinstance(failure_type, str) and failure_type in TYPES:
        result["failureType"] = failure_type
    failure = value.get("failure")
    if isinstance(failure, str) and failure in MESSAGES:
        result["failureId"] = "constant-refusal-" + str(MESSAGES.index(failure))
        result["failureMessage"] = failure
    rows = []
    stages = value.get("stages", [])
    if not isinstance(stages, list):
        stages = []
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        row = {}
        name = stage.get("name", stage.get("role"))
        if isinstance(name, str) and name in STAGES:
            row["stageId"] = name
        for key in ("exit", "exitCode", "stderrBytes"):
            if type(stage.get(key)) is int:
                row[key] = stage[key]
        checksum = stage.get("stderrSha256")
        if isinstance(checksum, str) and re.fullmatch("[0-9a-f]{64}", checksum):
            row["stderrSha256"] = checksum
        if row:
            rows.append(row)
    result["stages"] = rows
    return result

if __name__ == "__main__":
    base = Path(sys.argv[1])
    for relative in ("runtime/constructor.json", "runtime/peer-runtime-proof.json"):
        path = base / relative
        if path.is_symlink() or not path.is_file():
            print(json.dumps({"receipt": relative, "available": False}))
            continue
        print(json.dumps(summarize(json.loads(path.read_text()), relative), sort_keys=True))
