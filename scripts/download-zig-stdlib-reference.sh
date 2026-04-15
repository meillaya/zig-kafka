#!/usr/bin/env bash
set -euo pipefail

version="${1:-master}"
version="${version%/}"
base_url="https://ziglang.org/documentation/${version}/std"
jsonl_out=".tmp/zig-${version}-stdlib-docs.jsonl"
txt_out="docs/zig-${version}-stdlib-reference.txt"

mkdir -p .tmp docs

cat > .tmp/extract-zig-std-docs.js <<'JS'
const fs = require('fs');

const BASE_URL = process.env.ZIG_DOC_BASE_URL;
const JSONL_OUT = process.env.ZIG_STDLIB_JSONL_OUT;

const CAT = {
  namespace: 0,
  container: 1,
  global_variable: 2,
  function: 3,
  primitive: 4,
  error_set: 5,
  global_const: 6,
  alias: 7,
  type: 8,
  type_type: 9,
  type_function: 10,
};

const decoder = new TextDecoder();
let warningCount = 0;

function warn(context, err) {
  warningCount += 1;
  console.error(`[warn] ${context}: ${err?.message ?? err}`);
}

function safe(context, fallback, fn) {
  try {
    return fn();
  } catch (err) {
    warn(context, err);
    return fallback;
  }
}

function decodeString(mem, packed) {
  const ptr = Number(packed & 0xffffffffn);
  const len = Number(packed >> 32n);
  if (!len) return '';
  return decoder.decode(new Uint8Array(mem.buffer, ptr, len));
}

function unwrapSlice32(mem, packed) {
  const ptr = Number(packed & 0xffffffffn);
  const len = Number(packed >> 32n);
  if (!len) return [];
  return Array.from(new Uint32Array(mem.buffer, ptr, len));
}

function unwrapSlice64(mem, packed) {
  const ptr = Number(packed & 0xffffffffn);
  const len = Number(packed >> 32n);
  if (!len) return [];
  return Array.from(new BigUint64Array(mem.buffer, ptr, len));
}

async function fetchArrayBuffer(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch ${url}: ${response.status}`);
  return await response.arrayBuffer();
}

(async () => {
  const [wasmBuffer, sourcesBuffer] = await Promise.all([
    fetchArrayBuffer(`${BASE_URL}/main.wasm`),
    fetchArrayBuffer(`${BASE_URL}/sources.tar`),
  ]);

  const { instance } = await WebAssembly.instantiate(wasmBuffer, { js: { log() {} } });
  const w = instance.exports;

  const sources = new Uint8Array(sourcesBuffer);
  const ptr = w.alloc(sources.length);
  new Uint8Array(w.memory.buffer, ptr, sources.length).set(sources);
  w.unpack(ptr, sources.length);

  const modules = [];
  for (let i = 0;; i += 1) {
    const name = safe(`module_name(${i})`, '', () => decodeString(w.memory, w.module_name(i)));
    if (!name) break;
    modules.push({ index: i, name, root_decl: safe(`find_module_root(${i})`, -1, () => w.find_module_root(i)) });
  }

  const out = fs.createWriteStream(JSONL_OUT, { encoding: 'utf8' });
  let count = 0;
  const seen = new Set();

  function writeRecord(record) {
    out.write(JSON.stringify(record) + '\n');
    count += 1;
  }

  function walk(decl, moduleName, depth) {
    if (decl === -1 || seen.has(decl)) return;
    seen.add(decl);

    const rawCategoryNum = safe(`categorize_decl(${decl})`, -1, () => w.categorize_decl(decl, 0));
    const rawCategoryName = safe(`decl_category_name(${decl})`, '', () => decodeString(w.memory, w.decl_category_name(decl)));
    let effectiveDecl = decl;
    let effectiveCategoryNum = rawCategoryNum;
    let effectiveCategoryName = rawCategoryName;
    let aliasTargetDecl = null;

    if (rawCategoryNum === CAT.alias) {
      const aliasee = safe(`get_aliasee(${decl})`, -1, () => w.get_aliasee());
      if (aliasee !== -1) {
        aliasTargetDecl = aliasee;
        effectiveDecl = aliasee;
        effectiveCategoryNum = safe(`categorize_decl(alias ${effectiveDecl})`, rawCategoryNum, () => w.categorize_decl(effectiveDecl, 0));
        effectiveCategoryName = safe(`decl_category_name(alias ${effectiveDecl})`, rawCategoryName, () => decodeString(w.memory, w.decl_category_name(effectiveDecl)));
      }
    }

    const record = {
      module: moduleName,
      depth,
      decl,
      effective_decl: effectiveDecl,
      raw_fqn: safe(`decl_fqn(raw ${decl})`, '', () => decodeString(w.memory, w.decl_fqn(decl))),
      canonical_fqn: safe(`decl_fqn(effective ${effectiveDecl})`, '', () => decodeString(w.memory, w.decl_fqn(effectiveDecl))),
      raw_category: rawCategoryName,
      effective_category: effectiveCategoryName,
      raw_category_num: rawCategoryNum,
      effective_category_num: effectiveCategoryNum,
      alias_target_decl: aliasTargetDecl,
      file_path: safe(`decl_file_path(${effectiveDecl})`, '', () => decodeString(w.memory, w.decl_file_path(effectiveDecl))),
      type_html: (effectiveCategoryNum === CAT.function || effectiveCategoryNum === CAT.type_function)
        ? ''
        : safe(`decl_type_html(${effectiveDecl})`, '', () => decodeString(w.memory, w.decl_type_html(effectiveDecl))),
      fn_proto_html: (effectiveCategoryNum === CAT.function || effectiveCategoryNum === CAT.type_function)
        ? safe(`decl_fn_proto_html(${effectiveDecl})`, '', () => decodeString(w.memory, w.decl_fn_proto_html(effectiveDecl, false)))
        : '',
      docs_html: safe(`decl_docs_html(${effectiveDecl})`, '', () => decodeString(w.memory, w.decl_docs_html(effectiveDecl, false))),
      params_html: safe(`decl_params(${effectiveDecl})`, [], () =>
        unwrapSlice32(w.memory, w.decl_params(effectiveDecl)).map((param) =>
          safe(`decl_param_html(${effectiveDecl}, ${param})`, '', () => decodeString(w.memory, w.decl_param_html(effectiveDecl, param)))
        )
      ),
      fields_html: safe(`decl_fields(${effectiveDecl})`, [], () => {
        const fields = (effectiveCategoryNum === CAT.type_function)
          ? unwrapSlice32(w.memory, w.type_fn_fields(effectiveDecl))
          : unwrapSlice32(w.memory, w.decl_fields(effectiveDecl));
        return fields.map((field) =>
          safe(`decl_field_html(${effectiveDecl}, ${field})`, '', () => decodeString(w.memory, w.decl_field_html(effectiveDecl, field)))
        );
      }),
      doctest_html: safe(`decl_doctest_html(${effectiveDecl})`, '', () => decodeString(w.memory, w.decl_doctest_html(effectiveDecl))),
      errors_html: safe(`function_errors(${effectiveDecl})`, [], () => {
        if (effectiveCategoryNum !== CAT.function) return [];
        const errSetNode = w.fn_error_set(effectiveDecl);
        if (errSetNode === 0) return [];
        const baseDecl = w.fn_error_set_decl(effectiveDecl, errSetNode);
        return unwrapSlice64(w.memory, w.error_set_node_list(baseDecl, errSetNode)).map((node) =>
          safe(`error_html(${baseDecl}, ${node})`, '', () => decodeString(w.memory, w.error_html(baseDecl, node)))
        );
      }),
    };

    writeRecord(record);

    const children = safe(`children(${effectiveDecl})`, [], () => {
      if (effectiveCategoryNum === CAT.namespace || effectiveCategoryNum === CAT.container) {
        return unwrapSlice32(w.memory, w.namespace_members(effectiveDecl, false));
      }
      if (effectiveCategoryNum === CAT.type_function) {
        return unwrapSlice32(w.memory, w.type_fn_members(effectiveDecl, false));
      }
      return [];
    });

    for (const child of children) {
      walk(child, moduleName, depth + 1);
    }
  }

  for (const mod of modules) {
    walk(mod.root_decl, mod.name, 0);
  }

  out.end();
  out.on('finish', () => {
    console.log(JSON.stringify({ modules: modules.map((m) => m.name), records: count, warnings: warningCount, output: JSONL_OUT }));
  });
})();
JS

ZIG_DOC_BASE_URL="$base_url" ZIG_STDLIB_JSONL_OUT="$jsonl_out" node .tmp/extract-zig-std-docs.js

ZIG_DOC_VERSION="$version" ZIG_DOC_BASE_URL="$base_url" ZIG_STDLIB_JSONL_OUT="$jsonl_out" ZIG_STDLIB_TXT_OUT="$txt_out" python - <<'PY'
from pathlib import Path
import json, re, html as html_lib, os
from bs4 import BeautifulSoup
import html2text

version = os.environ['ZIG_DOC_VERSION']
base_url = os.environ['ZIG_DOC_BASE_URL']
src = Path(os.environ['ZIG_STDLIB_JSONL_OUT'])
out = Path(os.environ['ZIG_STDLIB_TXT_OUT'])

block = html2text.HTML2Text()
block.body_width = 0
block.ignore_images = True
block.inline_links = False
block.protect_links = False
block.ignore_emphasis = False


def strip_links(soup):
    for a in soup.find_all('a'):
        a.replace_with(a.get_text(' ', strip=False))
    return soup


def normalize(text: str) -> str:
    text = text.replace('Â§', '§')
    text = text.replace('\xa0', ' ')
    text = re.sub(r'\r\n?', '\n', text)
    text = re.sub(r'[ \t]+\n', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def block_text(fragment: str) -> str:
    if not fragment or not fragment.strip():
        return ''
    soup = strip_links(BeautifulSoup(f'<div>{fragment}</div>', 'html.parser'))
    text = block.handle(str(soup))
    text = html_lib.unescape(text)
    return normalize(text)


def inline_text(fragment: str) -> str:
    if not fragment or not fragment.strip():
        return ''
    soup = strip_links(BeautifulSoup(f'<div>{fragment}</div>', 'html.parser'))
    text = soup.get_text(' ', strip=True)
    text = html_lib.unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

count = 0
with out.open('w', encoding='utf-8') as f:
    f.write(f'Zig Standard Library Reference {version}\n')
    f.write(f'Source UI: {base_url}/\n')
    f.write('Autodoc assets: main.wasm + sources.tar\n\n')
    f.write('This file was generated by traversing the public std autodoc tree and converting declaration docs to plain text.\n\n')
    for line in src.open(encoding='utf-8'):
        rec = json.loads(line)
        count += 1
        heading = rec['raw_fqn'] or rec['canonical_fqn'] or f"decl-{rec['decl']}"
        f.write('=' * 80 + '\n')
        f.write(f'{heading}\n')
        f.write('=' * 80 + '\n')
        f.write(f"Module: {rec['module']}\n")
        f.write(f"Category: {rec['effective_category']}")
        if rec['raw_category_num'] == 7:
            f.write(f" (via alias")
            if rec['canonical_fqn'] and rec['canonical_fqn'] != heading:
                f.write(f": {rec['canonical_fqn']}")
            f.write(')')
        f.write('\n')
        if rec.get('file_path'):
            f.write(f"File: {rec['file_path']}\n")
        if rec.get('fn_proto_html'):
            sig = inline_text(rec['fn_proto_html'])
            if sig:
                f.write(f"Signature: {sig}\n")
        elif rec.get('type_html'):
            typ = inline_text(rec['type_html'])
            if typ:
                f.write(f"Type: {typ}\n")

        docs = block_text(rec.get('docs_html', ''))
        if docs:
            f.write('\nDocs:\n')
            f.write(docs + '\n')

        params = [block_text(x) for x in rec.get('params_html', [])]
        params = [p for p in params if p]
        if params:
            f.write('\nParameters:\n')
            for p in params:
                f.write(f'- {p}\n')

        fields = [block_text(x) for x in rec.get('fields_html', [])]
        fields = [p for p in fields if p]
        if fields:
            f.write('\nFields:\n')
            for p in fields:
                f.write(f'- {p}\n')

        errors = [block_text(x) for x in rec.get('errors_html', [])]
        errors = [e for e in errors if e]
        if errors:
            f.write('\nErrors:\n')
            for e in errors:
                f.write(f'- {e}\n')

        doctest = block_text(rec.get('doctest_html', ''))
        if doctest:
            f.write('\nExample Usage:\n')
            f.write(doctest + '\n')

        f.write('\n')

print(f'wrote {out}')
print(f'records: {count}')
print(f'bytes: {out.stat().st_size}')
PY
