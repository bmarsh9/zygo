/**
 * Zygo Variable Picker + Badge Renderer
 * Drop this <script> at the bottom of index.html, after Alpine.js loads.
 */

// ══════════════════════════════════════════════════════════════════════════════
// STYLES
// ══════════════════════════════════════════════════════════════════════════════
const BADGE_STYLES = `
.var-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 1px 7px 1px 6px;
  border-radius: 5px;
  font-size: 11px;
  font-family: 'Menlo', 'Monaco', monospace;
  line-height: 1.7;
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
  vertical-align: middle;
  position: relative;
  top: -1px;
  transition: filter .12s;
}
.var-badge:hover { filter: brightness(1.15); }
.var-badge-editing {
  display: inline-block;
  min-width: 60px;
  padding: 1px 6px;
  border-radius: 5px;
  font-size: 0.65rem;
  font-family: 'Menlo', 'Monaco', monospace;
  font-weight: 600;
  line-height: 1.7;
  vertical-align: middle;
  position: relative;
  top: -1px;
  outline: 2px solid oklch(var(--p) / 0.6);
  background: oklch(var(--b2));
  color: inherit;
  cursor: text;
  white-space: nowrap;
}
.var-badge::before {
  content: '';
  display: inline-block;
  width: 5px; height: 5px;
  border-radius: 50%;
  flex-shrink: 0;
  opacity: 0.6;
}
.var-badge-label { font-weight: 600; font-size: 0.65rem; }

.var-badge[data-type="http"]      { background: oklch(0.28 0.06 230 / 0.75); border: 1px solid oklch(0.50 0.12 230 / 0.55); color: oklch(0.88 0.07 230); }
.var-badge[data-type="http"]::before { background: oklch(0.65 0.14 230); }
.var-badge[data-type="db"]        { background: oklch(0.26 0.06 180 / 0.75); border: 1px solid oklch(0.48 0.12 180 / 0.55); color: oklch(0.88 0.07 180); }
.var-badge[data-type="db"]::before   { background: oklch(0.62 0.14 180); }
.var-badge[data-type="transform"] { background: oklch(0.26 0.07 150 / 0.75); border: 1px solid oklch(0.48 0.13 150 / 0.55); color: oklch(0.88 0.08 150); }
.var-badge[data-type="transform"]::before { background: oklch(0.65 0.16 150); }
.var-badge[data-type="default"]   { background: oklch(0.26 0.05 260 / 0.75); border: 1px solid oklch(0.46 0.10 260 / 0.55); color: oklch(0.88 0.05 260); }
.var-badge[data-type="default"]::before  { background: oklch(0.62 0.12 260); }

[data-theme="light"] .var-badge[data-type="http"]      { background: oklch(0.92 0.04 230); border-color: oklch(0.72 0.10 230); color: oklch(0.35 0.14 230); }
[data-theme="light"] .var-badge[data-type="db"]        { background: oklch(0.92 0.04 180); border-color: oklch(0.70 0.10 180); color: oklch(0.33 0.14 180); }
[data-theme="light"] .var-badge[data-type="transform"] { background: oklch(0.92 0.05 150); border-color: oklch(0.70 0.12 150); color: oklch(0.32 0.15 150); }
[data-theme="light"] .var-badge[data-type="default"]   { background: oklch(0.92 0.03 260); border-color: oklch(0.70 0.08 260); color: oklch(0.32 0.10 260); }

.badge-input {
  min-height: 36px;
  padding: 5px 10px;
  border: 1px solid oklch(var(--bc) / 0.2);
  border-radius: var(--rounded-btn, 0.5rem);
  background: transparent;
  outline: none;
  font-size: 13px;
  line-height: 1.8;
  cursor: text;
  transition: border-color .15s, box-shadow .15s;
  word-break: break-word;
  overflow-wrap: anywhere;
}
.badge-input:focus {
  border-color: oklch(var(--p) / 0.6);
  box-shadow: 0 0 0 2px oklch(var(--p) / 0.12);
}
.badge-input[data-placeholder]:empty::before {
  content: attr(data-placeholder);
  opacity: 0.35;
  pointer-events: none;
}

.var-picker-overlay {
  position: fixed; inset: 0; z-index: 99990; background: transparent;
}
.var-picker-modal {
  position: fixed; z-index: 99991;
  border-radius: 10px; overflow: hidden;
  display: flex; flex-direction: column;
  background: oklch(var(--b1));
  border: 1px solid oklch(var(--bc) / 0.18);
  box-shadow: 0 16px 48px oklch(0 0 0 / 0.35), 0 2px 8px oklch(0 0 0 / 0.15);
  animation: vPickerIn .1s ease-out both;
  /* Fixed height so the inner lists can flex+scroll correctly */
  height: 420px;
  max-height: 80vh;
}
@keyframes vPickerIn {
  from { opacity: 0; transform: translateY(-4px) scale(0.97); }
  to   { opacity: 1; transform: translateY(0) scale(1); }
}
.var-picker-panels {
  display: flex; flex: 1; min-height: 0; overflow: hidden;
}
.var-picker-step {
  display: flex; flex-direction: column; min-height: 0; overflow: hidden; flex-shrink: 0;
}
.var-picker-step-1 { width: 50%; border-right: 1px solid oklch(var(--bc) / 0.08); }
.var-picker-step-2 { width: 50%; }
.var-picker-step-header {
  padding: 8px 10px 6px;
  border-bottom: 1px solid oklch(var(--bc) / 0.08);
  flex-shrink: 0;
}
.var-picker-step-label {
  font-size: 9px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .08em; margin-bottom: 5px;
}
.var-picker-step-search {
  width: 100%; padding: 5px 9px; border-radius: 6px;
  border: 1px solid oklch(var(--bc) / 0.18);
  background: oklch(var(--b2)); font-size: 11.5px;
  outline: none; color: inherit; box-sizing: border-box;
}
.var-picker-step-search:focus { border-color: oklch(var(--p) / 0.5); }
.var-picker-step-list { flex: 1; overflow-y: auto; padding: 4px; scrollbar-width: thin; }

.var-picker-preview {
  padding: 6px 10px;
  border-bottom: 1px solid oklch(var(--bc) / 0.08);
  flex-shrink: 0;
}
.var-picker-expr-label {
  font-size: 9px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .07em; margin-bottom: 4px; opacity: 0.5;
}
.var-picker-expr-input {
  width: 100%; box-sizing: border-box;
  padding: 5px 9px;
  border-radius: 6px;
  border: 1px solid oklch(var(--p) / 0.35);
  background: oklch(var(--b2));
  font-size: 11.5px;
  font-family: 'Menlo', 'Monaco', monospace;
  outline: none;
  color: inherit;
  transition: border-color .15s;
}
.var-picker-expr-input:focus { border-color: oklch(var(--p) / 0.7); }
.var-picker-expr-hint {
  font-size: 9.5px; opacity: 0.4; margin-top: 4px;
}
.var-picker-insert-btn {
  display: flex; align-items: center; gap: 5px;
  margin-top: 7px; width: 100%; padding: 7px 12px;
  border-radius: 7px; border: none; cursor: pointer;
  background: oklch(var(--p)); color: oklch(var(--pc));
  font-size: 12px; font-weight: 600;
  transition: opacity .12s, transform .08s;
  justify-content: center;
}
.var-picker-insert-btn:hover  { opacity: 0.88; }
.var-picker-insert-btn:active { transform: scale(0.97); }
.var-picker-insert-btn svg { flex-shrink: 0; }
.var-picker-preview-op { display: none; }
.var-picker-preview-op.visible { display: none; }
.var-picker-preview-badge { display: none; }
.var-picker-preview-pipe { display: none; }

.var-picker-group-header {
  display: flex; align-items: center; gap: 6px;
  padding: 6px 6px 4px;
  cursor: pointer;
  user-select: none;
  border-radius: 5px;
  transition: background .08s;
  margin-top: 2px;
}
.var-picker-group-header:hover { background: oklch(var(--bc) / 0.05); }
.var-picker-group-name {
  font-size: 10px; font-weight: 700;
  flex: 1; min-width: 0;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.var-picker-group-count {
  font-size: 9px; opacity: 0.4;
  font-family: monospace; flex-shrink: 0;
}
.var-picker-group-chevron {
  flex-shrink: 0; opacity: 0.4; transition: transform .15s;
  width: 12px; height: 12px;
  display: flex; align-items: center; justify-content: center;
}
.var-picker-group-chevron svg { display: block; }
.var-picker-group-body {
  overflow: hidden;
  transition: height .15s ease;
}
.var-picker-group-label {
  font-size: 9px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .08em; opacity: 0.38; padding: 8px 6px 3px;
}
.var-picker-item {
  display: flex; align-items: center; gap: 8px;
  padding: 6px 8px; border-radius: 6px; cursor: pointer;
  border: 1px solid transparent; transition: background .08s;
}
.var-picker-item:hover, .var-picker-item.active {
  background: oklch(var(--p) / 0.09); border-color: oklch(var(--p) / 0.18);
}
.var-picker-item-path {
  font-size: 11px; font-family: 'Menlo','Monaco',monospace;
  flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis;
  white-space: nowrap; opacity: 0.6;
}
.var-picker-item-path em { font-style: normal; color: oklch(var(--p)); font-weight: 700; opacity: 1; }
.var-picker-item-example {
  font-size: 10px; opacity: 0.35; font-family: 'Menlo','Monaco',monospace;
  flex-shrink: 0; max-width: 70px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}

.var-picker-op-group {
  font-size: 9px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .08em; opacity: 0.38; padding: 8px 6px 3px;
}
.var-picker-op-item {
  display: flex; align-items: center; gap: 6px;
  padding: 6px 8px; border-radius: 6px; cursor: pointer;
  border: 1px solid transparent; transition: background .08s;
}
.var-picker-op-item:hover, .var-picker-op-item.active {
  background: oklch(var(--p) / 0.09); border-color: oklch(var(--p) / 0.18);
}
.var-picker-op-item.no-op {
  font-weight: 600; font-size: 11.5px; margin-bottom: 3px;
  border-color: oklch(var(--bc) / 0.1);
}
.var-picker-op-item.no-op:hover, .var-picker-op-item.no-op.active {
  background: oklch(var(--p) / 0.09); border-color: oklch(var(--p) / 0.3);
}
.var-picker-op-name {
  font-family: monospace; font-size: 11.5px; font-weight: 600;
  color: oklch(var(--p)); flex-shrink: 0; min-width: 80px;
}
.var-picker-op-desc { font-size: 10.5px; opacity: 0.45; flex: 1; text-align: right; }
.var-picker-no-op-raw { font-family: monospace; font-size: 10px; opacity: 0.35; margin-left: auto; }

.var-picker-empty {
  text-align: center; padding: 20px; font-size: 12px; opacity: 0.4; line-height: 1.6;
}
.var-picker-footer {
  padding: 5px 10px; border-top: 1px solid oklch(var(--bc) / 0.08);
  font-size: 10px; opacity: 0.35; flex-shrink: 0; display: flex; gap: 10px;
}
.var-picker-footer kbd {
  font-family: inherit; background: oklch(var(--b2));
  border: 1px solid oklch(var(--bc)/0.2); border-radius: 3px; padding: 0 3px; font-size: 9px;
}
`;

const styleEl = document.createElement('style');
styleEl.textContent = BADGE_STYLES;
document.head.appendChild(styleEl);


// ══════════════════════════════════════════════════════════════════════════════
// OPERATORS  –  loaded from /api/operators (served from operators.py API_SCHEMA)
// Shape: [{ group, items: [{ name, description, signature, args, example }] }]
// ══════════════════════════════════════════════════════════════════════════════

// Populated on first call to loadOperators()
let OPERATORS = [];

// Map operator names to their group for bucketing.
// Kept in sync with operators.py REGISTRY / API_SCHEMA.
const OP_GROUP_MAP = {
  STRING: ['STRING', 'UPPER', 'LOWER', 'TRIM', 'REPLACE', 'SLICE', 'CONCAT', 'JOIN'],
  NUMBER: ['INT', 'FLOAT', 'BOOL'],
  LOGIC:  ['IF'],
  DATE:   ['NOW', 'DATE'],
};

// Operators that take multiple arguments — selecting them inserts "OP({{path}}, )"
// with the cursor placed after the comma so the user can add more args immediately.
const VARIADIC_OPS = new Set(['CONCAT', 'JOIN', 'REPLACE', 'SLICE', 'IF']);

async function loadOperators() {
  try {
    const res = await fetch('/api/operators');
    const flat = await res.json(); // array of { name, description, signature, ... }

    const nameToGroup = {};
    Object.entries(OP_GROUP_MAP).forEach(([group, names]) => {
      names.forEach(n => { nameToGroup[n] = group; });
    });

    const grouped = {};
    flat.forEach(op => {
      const group = nameToGroup[op.name] || 'OTHER';
      if (!grouped[group]) grouped[group] = [];
      grouped[group].push(op);
    });

    // Preserve OP_GROUP_MAP order, then append OTHER if present
    const orderedGroups = [...Object.keys(OP_GROUP_MAP), 'OTHER'];
    OPERATORS = orderedGroups
      .filter(g => grouped[g])
      .map(g => ({ group: g, items: grouped[g] }));

  } catch (e) {
    console.warn('Zygo: could not load operators from /api/operators', e);
    OPERATORS = [];
  }
}


// ══════════════════════════════════════════════════════════════════════════════
// BADGE BUILDER
// ══════════════════════════════════════════════════════════════════════════════

function getBadgeType(path) {
  const s = (path || '').toLowerCase();
  if (['httprequest', 'request', 'webhook', 'http'].some(k => s.includes(k))) return 'http';
  if (['database', 'table', 'db'].some(k => s.includes(k))) return 'db';
  if (['transform', 'python', 'filter'].some(k => s.includes(k))) return 'transform';
  return 'default';
}

/**
 * Build a badge element.
 * raw is the full expression inside {{...}}, e.g.:
 *   "Request.token"          → plain variable
 *   "UPPER(Request.name)"    → operator call
 */
function buildBadgeEl(raw) {
  // Strip surrounding {{ }} if caller accidentally passed them — dataset.path
  // must always be the bare expression so _extractRaw can safely wrap it once.
  // Bare expression examples:  "Request.name"  "UPPER({{Request.name}})"
  raw = raw.trim();
  if (raw.startsWith('{{') && raw.endsWith('}}')) {
    raw = raw.slice(2, -2).trim();
  }

  // Match operator call: OP_NAME({{path}}) or OP_NAME(path)
  const fnMatch = raw.match(/^([A-Z_][A-Z0-9_]*)\(\{\{(.+?)\}\}(.*)\)$/)
               || raw.match(/^([A-Z_][A-Z0-9_]*)\(([^)]+)\)$/);
  const op       = fnMatch ? fnMatch[1] : null;
  // For display: extract the variable path from the first {{...}} if present
  const innerVar = fnMatch ? fnMatch[2].trim() : null;
  const displayLabel = op
    ? `${op}(${innerVar || fnMatch[2]})`
    : raw;

  const badge = document.createElement('span');
  badge.className = 'var-badge';
  badge.dataset.type = getBadgeType(op ? (innerVar || raw) : raw);
  badge.dataset.path = raw;   // bare expression — no {{ }} wrapper
  badge.contentEditable = 'false';
  badge.title = `{{${raw}}} — click to change`;

  const label = document.createElement('span');
  label.className = 'var-badge-label';
  label.textContent = displayLabel;
  badge.appendChild(label);

  return badge;
}

function tokenize(raw) {
  const out = [];
  const re = /\{\{([^}]+)\}\}/g;
  let last = 0, m;
  while ((m = re.exec(raw)) !== null) {
    if (m.index > last) out.push({ type: 'text', value: raw.slice(last, m.index) });
    out.push({ type: 'var', value: m[1].trim() });
    last = re.lastIndex;
  }
  if (last < raw.length) out.push({ type: 'text', value: raw.slice(last) });
  return out;
}

function escHtml(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}


// ══════════════════════════════════════════════════════════════════════════════
// VARIABLE PICKER MODAL  (two-step: variable → optional operator)
// ══════════════════════════════════════════════════════════════════════════════

class VariablePicker {
  constructor() {
    this._modal    = null;
    this._overlay  = null;
    this._onSelect = null;
    this._variables  = [];
    this._anchorRect = null;

    // Step 1
    this._s1Items         = [];
    this._s1ActiveIdx     = -1;
    this._s1SearchEl      = null;
    this._s1ListEl        = null;
    this._collapsedGroups = new Set(); // groups the user has manually collapsed
    this._expandedGroups  = new Set(); // groups the user has manually expanded

    // Step 2
    this._s2Path          = null;
    this._s2Items         = [];
    this._s2ActiveIdx     = -1;
    this._s2SearchEl      = null;
    this._s2ListEl        = null;
    this._s2PreviewOpBefore = null;
    this._s2PreviewOpParen  = null;
    this._s2PreviewOpClose  = null;
  }

  // ── Public ─────────────────────────────────────────────────────────────────

  // prefill: { prefillPath, prefillOp } — when editing an existing badge,
  // auto-navigate to the right step so the user sees the current value highlighted.
  show(anchorRect, variables, onSelect, prefill = {}) {
    this.destroy();
    this._onSelect   = onSelect;
    this._variables  = variables;
    this._anchorRect = anchorRect;
    this._s2Path     = null;

    this._buildModal();
    document.body.appendChild(this._overlay);
    document.body.appendChild(this._modal);

    if (prefill.prefillPath) {
      // Pre-expand the group containing this variable
      const root = prefill.prefillPath.split('.')[0];
      // Find nodeLabel matching this root so _expandedGroups key is correct
      const matchingVar = variables.find(v => v.path === prefill.prefillPath || v.path.startsWith(root + '.'));
      const groupKey = matchingVar?.nodeLabel || root;
      this._expandedGroups.add(groupKey);

      this._position(false);
      this._renderStep1('');

      // Single rAF: highlight variable, advance to step 2, set expr input
      requestAnimationFrame(() => {
        // Highlight the matching variable in step 1
        const items = [...this._s1ListEl.querySelectorAll('.var-picker-item')];
        items.forEach((el, i) => {
          const b = el.querySelector('.var-badge');
          if (b?.dataset?.path === prefill.prefillPath) {
            this._s1SetActive(i);
            el.scrollIntoView({ block: 'nearest' });
          }
        });

        // Auto-advance to Step 2
        this._pickVariable(prefill.prefillPath);

        // Override what _pickVariable set — use the FULL original expression
        const fullExpr = prefill.prefillOp
          ? `${prefill.prefillOp}({{${prefill.prefillPath}}})`
          : `{{${prefill.prefillPath}}}`;
        if (this._s2ExprInput) {
          this._s2ExprInput.value = fullExpr;
          this._s2ExprInput.focus();
          this._s2ExprInput.select();
        }

        // Highlight matching operator in list for visual reference
        if (prefill.prefillOp) {
          const opItems = [...this._s2ListEl.querySelectorAll('.var-picker-op-item:not(.no-op)')];
          opItems.forEach((el, i) => {
            if (el.querySelector('.var-picker-op-name')?.textContent === prefill.prefillOp) {
              this._s2SetActive(i, el);
              el.scrollIntoView({ block: 'nearest' });
            }
          });
        } else {
          const noOp = this._s2ListEl.querySelector('.no-op');
          if (noOp) this._s2SetActive(-1, noOp);
        }
      });
    } else {
      this._position(false);
      this._renderStep1('');
      requestAnimationFrame(() => this._s1SearchEl?.focus());
    }
  }

  destroy() {
    this._overlay?.remove();
    this._modal?.remove();
    this._overlay = null;
    this._modal   = null;
  }

  get isOpen() { return !!this._modal; }

  // ── Modal construction ──────────────────────────────────────────────────────

  _buildModal() {
    this._overlay = document.createElement('div');
    this._overlay.className = 'var-picker-overlay';
    this._overlay.addEventListener('mousedown', (e) => {
      if (!this._modal?.contains(e.target)) this.destroy();
    });

    this._modal = document.createElement('div');
    this._modal.className = 'var-picker-modal';

    const panels = document.createElement('div');
    panels.className = 'var-picker-panels';

    // ── Step 1 ──
    const step1 = document.createElement('div');
    step1.className = 'var-picker-step var-picker-step-1';

    const s1Header = document.createElement('div');
    s1Header.className = 'var-picker-step-header';
    s1Header.innerHTML = `<div class="var-picker-step-label">Step 1 — Pick variable</div>
      <input class="var-picker-step-search" placeholder="Search variables…" autocomplete="off" spellcheck="false" />`;
    this._s1SearchEl = s1Header.querySelector('input');

    this._s1ListEl = document.createElement('div');
    this._s1ListEl.className = 'var-picker-step-list';
    step1.appendChild(s1Header);
    step1.appendChild(this._s1ListEl);

    // ── Step 2 ──
    this._step2El = document.createElement('div');
    this._step2El.className = 'var-picker-step var-picker-step-2';
    this._step2El.style.display = 'none';

    const s2Header = document.createElement('div');
    s2Header.className = 'var-picker-step-header';
    s2Header.innerHTML = `<div class="var-picker-step-label">Step 2 — Pick operator (optional)</div>`;

    // Expression editor — shows the full raw expression, user can type directly
    this._s2PreviewRow = document.createElement('div');
    this._s2PreviewRow.className = 'var-picker-preview';
    this._s2PreviewRow.innerHTML = `
      <div class="var-picker-expr-label">Expression</div>
      <input class="var-picker-expr-input" type="text" spellcheck="false" autocomplete="off" placeholder="e.g. {{Request.name}} or UPPER({{Request.name}})" />
      <div class="var-picker-expr-hint">Edit directly or pick an operator below</div>
      <button class="var-picker-insert-btn" type="button">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round">
          <polyline points="9 18 15 12 9 6"/>
        </svg>
        Insert
      </button>
    `;
    this._s2ExprInput = this._s2PreviewRow.querySelector('.var-picker-expr-input');
    // Wire Insert button
    this._s2PreviewRow.querySelector('.var-picker-insert-btn').addEventListener('mousedown', (e) => {
      e.preventDefault();
      this._insertExprValue();
    });

    // Wire Enter/Escape on the expr input
    this._s2ExprInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        this._insertExprValue();
      }
      if (e.key === 'Escape') {
        e.preventDefault();
        this._handleKey(e);
      }
    });

    // Keep dummy refs so existing code that touches these doesn't throw
    this._s2PreviewOpBefore = { textContent: '', classList: { add(){}, remove(){} } };
    this._s2PreviewOpParen  = { classList: { add(){}, remove(){} } };
    this._s2PreviewOpClose  = { classList: { add(){}, remove(){} } };
    this._s2PreviewBadgeSlot = { innerHTML: '', appendChild(){} };

    const s2SearchWrap = document.createElement('div');
    s2SearchWrap.style.padding = '5px 10px 6px';
    s2SearchWrap.innerHTML = `<input class="var-picker-step-search" placeholder="Search operators…" autocomplete="off" spellcheck="false" />`;
    this._s2SearchEl = s2SearchWrap.querySelector('input');

    this._s2ListEl = document.createElement('div');
    this._s2ListEl.className = 'var-picker-step-list';

    this._step2El.appendChild(s2Header);
    this._step2El.appendChild(this._s2PreviewRow);
    this._step2El.appendChild(s2SearchWrap);
    this._step2El.appendChild(this._s2ListEl);

    panels.appendChild(step1);
    panels.appendChild(this._step2El);

    // Footer
    const footer = document.createElement('div');
    footer.className = 'var-picker-footer';
    this._footerEl = footer;
    this._updateFooter(1);

    this._modal.appendChild(panels);
    this._modal.appendChild(footer);

    // Wire step 1
    this._s1SearchEl.addEventListener('input', () => {
      this._renderStep1(this._s1SearchEl.value.trim().toLowerCase());
    });
    this._s1SearchEl.addEventListener('keydown', (e) => this._handleKey(e));

    // Wire step 2
    this._s2SearchEl.addEventListener('input', () => {
      this._renderStep2Ops(this._s2SearchEl.value.trim().toLowerCase());
    });
    this._s2SearchEl.addEventListener('keydown', (e) => this._handleKey(e));
  }

  // ── Positioning ─────────────────────────────────────────────────────────────

  _position(wide) {
    const rect = this._anchorRect;
    const vw = window.innerWidth, vh = window.innerHeight;
    const mw = Math.floor(vw * 0.5);
    const mh = 420;
    const margin = 8;

    // Vertical: prefer below anchor, flip above if it would clip
    let top = (rect.bottom ?? rect.top) + 6;
    if (top + mh > vh - margin) top = Math.max(margin, (rect.top ?? 0) - mh - 6);
    top = Math.max(margin, top);

    // Horizontal: try anchoring to rect.left, but clamp so modal stays on screen.
    // If the modal is wider than the viewport just pin to the left margin.
    let left = rect.left ?? 0;
    left = Math.min(left, vw - mw - margin); // don't overflow right
    left = Math.max(margin, left);            // don't overflow left

    this._modal.style.top   = top + 'px';
    this._modal.style.left  = left + 'px';
    this._modal.style.width = Math.min(mw, vw - margin * 2) + 'px';
  }

  // ── Step 1 ──────────────────────────────────────────────────────────────────

  _renderStep1(q) {
    const list = this._s1ListEl;
    list.innerHTML = '';
    this._s1Items     = [];
    this._s1ActiveIdx = -1;

    const vars = q
      ? this._variables.filter(v => v.path.toLowerCase().includes(q))
      : this._variables;

    if (!vars.length) {
      list.innerHTML = `<div class="var-picker-empty">No variables found.<br><span style="font-size:10px">Run upstream nodes first.</span></div>`;
      return;
    }

    // Group by nodeLabel (enriched in getUpstreamVariables) or root path segment
    const groupMap = new Map();
    vars.forEach(v => {
      const root = v.path.split('.')[0];
      const groupKey = v.nodeLabel || root;
      if (!groupMap.has(groupKey)) groupMap.set(groupKey, []);
      groupMap.get(groupKey).push(v);
    });

    groupMap.forEach((items, groupName) => {
      // When searching, always expand so results are visible.
      // When not searching, collapsed by default unless the user has opened it.
      const isSearching = !!q;
      const isCollapsed = !isSearching && !this._expandedGroups.has(groupName);

      // ── Group header ──
      const header = document.createElement('div');
      header.className = 'var-picker-group-header';

      const chevron = document.createElement('span');
      chevron.className = 'var-picker-group-chevron';
      chevron.innerHTML = `<svg width="10" height="10" viewBox="0 0 24 24" fill="none"
        stroke="currentColor" stroke-width="2.5" stroke-linecap="round">
        <polyline points="6 9 12 15 18 9"/>
      </svg>`;
      chevron.style.transform = isCollapsed ? 'rotate(-90deg)' : 'rotate(0deg)';

      const nameEl = document.createElement('span');
      nameEl.className = 'var-picker-group-name';
      nameEl.textContent = groupName;

      const countEl = document.createElement('span');
      countEl.className = 'var-picker-group-count';
      countEl.textContent = items.length + ' var' + (items.length !== 1 ? 's' : '');

      header.appendChild(chevron);
      header.appendChild(nameEl);
      header.appendChild(countEl);

      // ── Group body (collapsible) ──
      const body = document.createElement('div');
      body.className = 'var-picker-group-body';

      items.forEach(v => {
        const idx = this._s1Items.length;
        this._s1Items.push(v);

        const item = document.createElement('div');
        item.className = 'var-picker-item';
        item.style.paddingLeft = '18px'; // indent under group header

        const badgeClone = buildBadgeEl(v.path);
        badgeClone.style.fontSize      = '10px';
        badgeClone.style.padding       = '0px 5px';
        badgeClone.style.pointerEvents = 'none';
        item.appendChild(badgeClone);

        const pathEl = document.createElement('span');
        pathEl.className = 'var-picker-item-path';
        // Show only the key part (after the root node name) for clarity
        const keyPart = v.path.includes('.') ? v.path.slice(v.path.indexOf('.') + 1) : v.path;
        if (q) {
          const display = v.path;
          const qi = display.toLowerCase().indexOf(q);
          if (qi > -1) {
            pathEl.innerHTML = escHtml(display.slice(0, qi))
              + `<em>${escHtml(display.slice(qi, qi + q.length))}</em>`
              + escHtml(display.slice(qi + q.length));
          } else {
            pathEl.textContent = keyPart;
          }
        } else {
          pathEl.textContent = keyPart;
        }
        item.appendChild(pathEl);

        if (v.example != null) {
          const ex = document.createElement('span');
          ex.className = 'var-picker-item-example';
          ex.textContent = String(v.example).slice(0, 16);
          item.appendChild(ex);
        }

        item.addEventListener('mousedown', (e) => { e.preventDefault(); this._pickVariable(v.path); });
        item.addEventListener('mouseenter', () => this._s1SetActive(idx));
        body.appendChild(item);
      });

      // ── Collapse/expand logic ──
      // Set initial height
      if (isCollapsed) {
        body.style.height = '0px';
      }
      // Animate after appending so we get correct scrollHeight
      const toggleGroup = () => {
        const collapsed = !this._expandedGroups.has(groupName);
        if (collapsed) {
          // Expand
          this._expandedGroups.add(groupName);
          chevron.style.transform = 'rotate(0deg)';
          body.style.height = body.scrollHeight + 'px';
          body.addEventListener('transitionend', () => {
            if (this._expandedGroups.has(groupName)) body.style.height = 'auto';
          }, { once: true });
        } else {
          // Collapse: set explicit px first so CSS transition works from auto
          body.style.height = body.scrollHeight + 'px';
          requestAnimationFrame(() => {
            this._expandedGroups.delete(groupName);
            chevron.style.transform = 'rotate(-90deg)';
            body.style.height = '0px';
          });
        }
      };

      header.addEventListener('mousedown', (e) => {
        e.preventDefault(); // don't steal focus from search
        toggleGroup();
      });

      list.appendChild(header);
      list.appendChild(body);
    });

    if (this._s1Items.length) this._s1SetActive(0);
  }

  _s1SetActive(idx) {
    this._s1ActiveIdx = idx;
    this._s1ListEl.querySelectorAll('.var-picker-item').forEach((el, i) => {
      el.classList.toggle('active', i === idx);
    });
    this._s1ListEl.querySelectorAll('.var-picker-item')[idx]?.scrollIntoView({ block: 'nearest' });
  }

  // ── Step 2 ──────────────────────────────────────────────────────────────────

  _pickVariable(path) {
    this._s2Path = path;

    this._step2El.style.display = '';
    this._position(true);

    // Highlight selected variable in step 1
    this._s1ListEl.querySelectorAll('.var-picker-item').forEach(el => {
      const badge = el.querySelector('.var-badge');
      el.classList.toggle('active', badge?.dataset?.path === path);
    });

    // Populate the expression input with {{path}} as the starting value
    if (this._s2ExprInput) {
      this._s2ExprInput.value = `{{${path}}}`;
    }

    this._renderStep2Ops('');
    this._s2SearchEl.value = '';
    requestAnimationFrame(() => this._s2ExprInput?.focus());
    this._updateFooter(2);
  }

  _renderStep2Ops(q) {
    const list = this._s2ListEl;
    list.innerHTML = '';
    this._s2Items     = [];
    this._s2ActiveIdx = -1;

    // Always first: Insert without operator
    if (!q) {
      const noOp = document.createElement('div');
      noOp.className = 'var-picker-op-item no-op';
      noOp.innerHTML = `<span style="flex:1;font-size:11.5px">Insert without operator</span>
        <span class="var-picker-no-op-raw">{{${escHtml(this._s2Path)}}}</span>`;
      noOp.addEventListener('mousedown', (e) => {
        e.preventDefault();
        // Reset expr to plain {{path}} then insert
        if (this._s2ExprInput && this._s2Path) {
          this._s2ExprInput.value = `{{${this._s2Path}}}`;
        }
        this._insertExprValue();
      });
      noOp.addEventListener('mouseenter', () => {
        // Preview: reset expr to plain {{path}}
        if (this._s2ExprInput && this._s2Path) {
          this._s2ExprInput.value = `{{${this._s2Path}}}`;
        }
        this._s2SetActive(-1, noOp);
      });
      list.appendChild(noOp);
      this._s2Items.push({ type: 'no-op' });
    }

    // Filter operator groups
    const allOps = [];
    OPERATORS.forEach(g => {
      const filtered = q ? g.items.filter(op =>
        op.name.toLowerCase().includes(q) ||
        (op.description || '').toLowerCase().includes(q)
      ) : g.items;
      if (filtered.length) allOps.push({ group: g.group, items: filtered });
    });

    if (!allOps.length) {
      if (q) {
        const empty = document.createElement('div');
        empty.className = 'var-picker-empty';
        empty.textContent = 'No operators match.';
        list.appendChild(empty);
      }
      return;
    }

    allOps.forEach(g => {
      const lbl = document.createElement('div');
      lbl.className = 'var-picker-op-group';
      lbl.textContent = g.group;
      list.appendChild(lbl);

      g.items.forEach(op => {
        const idx = this._s2Items.length;
        this._s2Items.push({ type: 'op', op });

        const item = document.createElement('div');
        item.className = 'var-picker-op-item';
        if (op.signature) item.title = op.signature;

        const nameEl = document.createElement('span');
        nameEl.className = 'var-picker-op-name';
        nameEl.textContent = op.name;

        const descEl = document.createElement('span');
        descEl.className = 'var-picker-op-desc';
        descEl.textContent = op.description || '';

        item.appendChild(nameEl);
        item.appendChild(descEl);

        item.addEventListener('mousedown', (e) => {
          e.preventDefault();
          // Update the expression input with this operator applied
          this._applyOpToExpr(op.name);
          this._s2SetActive(idx, item);
        });
        item.addEventListener('mouseenter', () => { this._s2SetActive(idx, item); });

        list.appendChild(item);
      });
    });

    // Auto-highlight no-op on fresh open
    if (!q) {
      const noOpEl = list.querySelector('.no-op');
      if (noOpEl) this._s2SetActive(-1, noOpEl);
    }
  }

  _s2SetActive(idx, el) {
    this._s2ActiveIdx = idx;
    this._s2ListEl.querySelectorAll('.var-picker-op-item').forEach(e => e.classList.remove('active'));
    if (el) { el.classList.add('active'); el.scrollIntoView({ block: 'nearest' }); }
  }

  // ── Final selection ──────────────────────────────────────────────────────────

  _finalSelect(path, operator) {
    // Always defer to the expr input — it is the canonical value.
    // _applyOpToExpr already updated it when the operator was selected.
    const val = this._s2ExprInput?.value?.trim() || (operator ? `${operator}({{${path}}})` : `{{${path}}}`);
    this._onSelect?.(val, {});
    this.destroy();
  }

  // Called when user clicks an operator item — wraps the current variable in the operator
  _applyOpToExpr(opName) {
    if (!this._s2ExprInput) return;
    const current = this._s2ExprInput.value.trim();
    if (!current) return;

    // Extract the core variable reference from the current expr input.
    // Could be: "{{path}}", "OP({{path}})", "OP({{path}}, extra)"
    // We want to grab just the {{path}} part to re-wrap with the new operator.
    const varMatch = current.match(/\{\{([^}]+)\}\}/);
    const varRef = varMatch ? `{{${varMatch[1]}}}` : current;

    const isVariadic = VARIADIC_OPS.has(opName);
    if (isVariadic) {
      // Place ", )" after the variable ref; cursor before ")"
      this._s2ExprInput.value = `${opName}(${varRef}, )`;
      const pos = this._s2ExprInput.value.length - 1;
      this._s2ExprInput.setSelectionRange(pos, pos);
    } else {
      this._s2ExprInput.value = `${opName}(${varRef})`;
    }
    this._s2ExprInput.focus();
  }

  // ── Keyboard ─────────────────────────────────────────────────────────────────

  _handleKey(e) {
    const active = document.activeElement;
    const inStep2 = active === this._s2SearchEl || active === this._s2ExprInput;

    if (e.key === 'Escape') {
      e.preventDefault();
      if (this._s2Path) {
        // Back to step 1
        this._step2El.style.display = 'none';
        this._position(false);
        this._s2Path = null;
        this._updateFooter(1);
        requestAnimationFrame(() => this._s1SearchEl?.focus());
      } else {
        this.destroy();
      }
      return;
    }

    if (inStep2) {
      // Arrow keys navigate the operator list; Enter/Tab insert
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        // Only intercept arrows when focus is on the search, not the expr input
        // (so the expr input still lets user move the cursor with arrows)
        if (active !== this._s2ExprInput) {
          const allItems = [...this._s2ListEl.querySelectorAll('.var-picker-op-item')];
          const count = allItems.length;
          if (!count) return;
          const cur = allItems.findIndex(el => el.classList.contains('active'));
          e.preventDefault();
          if (e.key === 'ArrowDown') this._activateOpItem(allItems[Math.min(cur + 1, count - 1)], Math.min(cur + 1, count - 1));
          else                        this._activateOpItem(allItems[Math.max(cur - 1, 0)], Math.max(cur - 1, 0));
        }
      } else if (e.key === 'Enter') {
        // Don't intercept Enter on expr input — it's handled by its own listener
        if (active !== this._s2ExprInput) {
          e.preventDefault();
          this._insertExprValue();
        }
      } else if (e.key === 'Tab') {
        e.preventDefault();
        this._insertExprValue();
      }
    } else {
      const count = this._s1Items.length;
      if (e.key === 'ArrowDown')  { e.preventDefault(); this._s1SetActive(Math.min(this._s1ActiveIdx + 1, count - 1)); }
      else if (e.key === 'ArrowUp')  { e.preventDefault(); this._s1SetActive(Math.max(this._s1ActiveIdx - 1, 0)); }
      else if (e.key === 'Enter')    { e.preventDefault(); if (this._s1ActiveIdx >= 0) this._pickVariable(this._s1Items[this._s1ActiveIdx].path); }
      else if (e.key === 'Tab')      { e.preventDefault(); if (this._s1ActiveIdx >= 0) this._pickVariable(this._s1Items[this._s1ActiveIdx].path); }
    }
  }

  // Insert the current expr input value — single source of truth for insertion
  _insertExprValue() {
    const val = this._s2ExprInput?.value?.trim();
    if (!val) return;
    this._onSelect?.(val, {});
    this.destroy();
  }

  _activateOpItem(el, idx) {
    if (!el) return;
    if (el.classList.contains('no-op')) {
      // Restore expr input to plain {{path}} on no-op
      if (this._s2ExprInput && this._s2Path) {
        this._s2ExprInput.value = `{{${this._s2Path}}}`;
      }
      this._s2SetActive(-1, el);
    } else {
      const opName = el.querySelector('.var-picker-op-name')?.textContent || '';
      this._applyOpToExpr(opName);
      this._s2SetActive(idx, el);
    }
  }

  _updateFooter(step) {
    if (!this._footerEl) return;
    this._footerEl.innerHTML = step === 1
      ? `<span><kbd>↑↓</kbd> navigate</span><span><kbd>↵</kbd> next</span><span><kbd>Tab</kbd> insert</span><span><kbd>Esc</kbd> close</span>`
      : `<span><kbd>↑↓</kbd> navigate</span><span><kbd>↵</kbd> insert</span><span><kbd>Esc</kbd> back</span>`;
  }
}

const picker = new VariablePicker();


// ══════════════════════════════════════════════════════════════════════════════
// BADGE-INPUT  –  contenteditable div that stores raw {{}} text
// ══════════════════════════════════════════════════════════════════════════════

class BadgeInput {
  constructor(container, rawValue, opts = {}) {
    this.el   = container;
    this.opts = opts;
    this._raw = rawValue || '';
    this._ignoreInput = false;
    this._savedRange  = null;

    container.classList.add('badge-input');
    container.contentEditable = 'true';
    container.spellcheck = false;
    if (opts.placeholder) container.dataset.placeholder = opts.placeholder;

    this._render(this._raw);
    this._bind();
  }

  getRaw() { return this._extractRaw(); }
  setRaw(raw) { this._raw = raw; this._render(raw); }

  _render(raw) {
    this._ignoreInput = true;
    this.el.innerHTML = '';
    tokenize(raw).forEach(tok => {
      if (tok.type === 'text') {
        this.el.appendChild(document.createTextNode(tok.value));
      } else {
        const badge = buildBadgeEl(tok.value);
        this._attachBadgeClick(badge);
        this.el.appendChild(badge);
      }
    });
    this._ignoreInput = false;
  }

  _attachBadgeClick(badge) {
    // Single-click → open picker pre-filled with current value
    badge.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      const vars = this.opts.getVariables?.() || [];
      const rect = badge.getBoundingClientRect();

      const existingRaw = badge.dataset.path || '';
      // existingRaw is e.g. "Request.name" or "UPPER({{Request.name}})"
      // Extract op and bare path (no {{ }}) for prefill
      const fnMatch = existingRaw.match(/^([A-Z_][A-Z0-9_]*)\(\{\{(.+?)\}\}.*\)$/);
      if (fnMatch) {
        // Operator call: UPPER({{Request.name}}) → op=UPPER, path=Request.name
        var existingOp   = fnMatch[1];
        var existingPath = fnMatch[2].trim();
      } else {
        // Plain variable — may or may not have {{ }}
        var existingOp   = null;
        var existingPath = existingRaw.replace(/^\{\{|\}\}$/g, '').trim();
      }

      picker.show(rect, vars, (raw, opts) => {
        const newBadge = buildBadgeEl(raw);
        this._attachBadgeClick(newBadge);
        badge.replaceWith(newBadge);
        this._sync();
        const range = document.createRange();
        range.setStartAfter(newBadge);
        range.collapse(true);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
        this.el.focus();
      }, { prefillPath: existingPath, prefillOp: existingOp });
    });

    // Double-click → inline text edit
    badge.addEventListener('dblclick', (e) => {
      e.preventDefault();
      e.stopPropagation();
      picker.destroy(); // close picker if open

      const currentRaw = badge.dataset.path || '';

      // Replace badge with an inline contenteditable span
      const editor = document.createElement('span');
      editor.className = 'var-badge-editing';
      editor.contentEditable = 'true';
      editor.spellcheck = false;
      editor.textContent = currentRaw;
      badge.replaceWith(editor);

      // Select all text so user can immediately retype
      const range = document.createRange();
      range.selectNodeContents(editor);
      const sel = window.getSelection();
      sel.removeAllRanges();
      sel.addRange(range);
      editor.focus();

      const commit = () => {
        const newRaw = editor.textContent.trim();
        // Build a new badge (or just remove if empty)
        if (newRaw) {
          const newBadge = buildBadgeEl(newRaw);
          this._attachBadgeClick(newBadge);
          editor.replaceWith(newBadge);
        } else {
          editor.remove();
        }
        this._sync();
      };

      editor.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') { e.preventDefault(); commit(); }
        if (e.key === 'Escape') {
          // Restore original badge on cancel
          const restored = buildBadgeEl(currentRaw);
          this._attachBadgeClick(restored);
          editor.replaceWith(restored);
        }
      });

      // Only commit on blur if the picker hasn't just been opened
      editor.addEventListener('blur', (e) => {
        // If focus moved to the picker modal, don't commit — let picker handle it
        setTimeout(() => {
          if (!picker.isOpen) commit();
        }, 100);
      }, { once: true });
    });
  }

  _extractRaw() {
    let raw = '';
    this.el.childNodes.forEach(node => {
      if (node.nodeType === Node.TEXT_NODE) {
        raw += node.textContent.replace(/\u200B/g, '');
      } else if (node.nodeType === Node.ELEMENT_NODE) {
        const path = node.dataset?.path;
        raw += path != null ? `{{${path}}}` : node.textContent;
      }
    });
    return raw;
  }

  _sync() {
    const raw = this._extractRaw();
    this._raw = raw;
    this.opts.onChange?.(raw);

    // Re-render if a text node contains a complete {{...}} that isn't yet a badge
    const hasUnrenderedVar = Array.from(this.el.childNodes).some(
      n => n.nodeType === Node.TEXT_NODE && /\{\{[^}]+\}\}/.test(n.textContent)
    );
    if (hasUnrenderedVar) {
      const offset = this._caretCharOffset();
      this._render(raw);
      this._restoreCaretCharOffset(offset);
    }
  }

  // opts.isVariadic — insert "OP({{path}}, )" with cursor between ", " and ")"
  insertVariable(path, opts = {}) {
    const range = this._savedRange || (() => {
      const s = window.getSelection();
      return s?.rangeCount ? s.getRangeAt(0) : null;
    })();
    this._savedRange = null;

    if (!range || !this.el.contains(range.startContainer)) {
      const badge = buildBadgeEl(path);
      this._attachBadgeClick(badge);
      this.el.appendChild(badge);
      if (opts.isVariadic) this.el.appendChild(document.createTextNode(', )'));
      this._sync();
      this.el.focus();
      return;
    }

    // Remove the '{{' trigger characters that opened the picker
    if (range.startContainer.nodeType === Node.TEXT_NODE) {
      const node = range.startContainer;
      const pos  = range.startOffset;
      const text = node.textContent;
      if (pos >= 2 && text[pos - 1] === '{' && text[pos - 2] === '{') {
        node.textContent = text.slice(0, pos - 2) + text.slice(pos);
        range.setStart(node, pos - 2);
        range.setEnd(node, pos - 2);
      }
    }

    const badge = buildBadgeEl(path);
    this._attachBadgeClick(badge);
    range.deleteContents();
    range.insertNode(badge);

    if (opts.isVariadic) {
      // Place ", )" after badge; cursor lands between ", " and ")" so the user
      // can immediately type a literal or press "/" to pick another variable
      const trailing = document.createTextNode(', )');
      range.setStartAfter(badge);
      range.insertNode(trailing);
      const newRange = document.createRange();
      newRange.setStart(trailing, 2); // after ", ", before ")"
      newRange.collapse(true);
      this.el.focus();
      const sel = window.getSelection();
      sel.removeAllRanges();
      sel.addRange(newRange);
    } else {
      const after = document.createTextNode('\u200B');
      range.setStartAfter(badge);
      range.insertNode(after);
      range.setStartAfter(after);
      range.collapse(true);
      this.el.focus();
      const sel = window.getSelection();
      sel.removeAllRanges();
      sel.addRange(range);
    }

    this._sync();
  }

  _bind() {
    const el = this.el;

    el.addEventListener('input', () => {
      if (this._ignoreInput) return;
      this._sync();
    });

    el.addEventListener('keydown', (e) => {
      if (e.key === 'Backspace' || e.key === 'Delete') {
        const badge = this._adjacentBadge(e.key === 'Backspace' ? 'before' : 'after');
        if (badge) { e.preventDefault(); badge.remove(); this._sync(); }
      }

      // Trigger variable picker on '{{' (double open brace)
      if (e.key === '{') {
        setTimeout(() => {
          const sel = window.getSelection();
          if (!sel?.rangeCount) return;
          const node = sel.anchorNode;
          const offset = sel.anchorOffset;

          // Only trigger if the character before the just-typed '{' is also '{'
          if (node?.nodeType !== Node.TEXT_NODE || offset < 2) return;
          if (node.textContent[offset - 1] !== '{' || node.textContent[offset - 2] !== '{') return;

          this._savedRange = sel.getRangeAt(0).cloneRange();
          const rect = this._caretRect();
          const vars = this.opts.getVariables?.() || [];
          console.log('[Zygo] {{ typed, vars:', vars, 'rect:', rect);

          picker.show(rect, vars, (path, opts) => this.insertVariable(path, opts));
        }, 0);
      }

      if (e.key === 'Escape') picker.destroy();
    });

    el.addEventListener('paste', (e) => {
      e.preventDefault();
      const text = e.clipboardData.getData('text/plain');
      if (!text) return;
      const sel = window.getSelection();
      if (!sel?.rangeCount) return;
      const range = sel.getRangeAt(0);
      range.deleteContents();
      const frag = document.createDocumentFragment();
      let lastNode = null;
      tokenize(text).forEach(tok => {
        if (tok.type === 'text') {
          lastNode = document.createTextNode(tok.value);
          frag.appendChild(lastNode);
        } else {
          const badge = buildBadgeEl(tok.value);
          this._attachBadgeClick(badge);
          frag.appendChild(badge);
          lastNode = document.createTextNode('\u200B');
          frag.appendChild(lastNode);
        }
      });
      range.insertNode(frag);
      if (lastNode) {
        const r = document.createRange();
        r.setStartAfter(lastNode);
        r.collapse(true);
        sel.removeAllRanges();
        sel.addRange(r);
      }
      this._sync();
    });
  }

  _adjacentBadge(dir) {
    const sel = window.getSelection();
    if (!sel?.rangeCount || !sel.getRangeAt(0).collapsed) return null;
    const { startContainer: node, startOffset: offset } = sel.getRangeAt(0);
    if (dir === 'before') {
      if (node.nodeType === Node.TEXT_NODE && offset === 0)
        return node.previousSibling?.classList?.contains('var-badge') ? node.previousSibling : null;
      if (node.nodeType === Node.ELEMENT_NODE)
        return node.childNodes[offset - 1]?.classList?.contains('var-badge') ? node.childNodes[offset - 1] : null;
    } else {
      if (node.nodeType === Node.TEXT_NODE && offset === node.textContent.length)
        return node.nextSibling?.classList?.contains('var-badge') ? node.nextSibling : null;
      if (node.nodeType === Node.ELEMENT_NODE)
        return node.childNodes[offset]?.classList?.contains('var-badge') ? node.childNodes[offset] : null;
    }
    return null;
  }

  _caretRect() {
    const sel = window.getSelection();
    if (sel?.rangeCount) {
      const r = sel.getRangeAt(0).cloneRange();
      r.collapse(true);
      const rect = r.getBoundingClientRect();
      if (rect.top || rect.left) return rect;
    }
    return this.el.getBoundingClientRect();
  }

  _caretCharOffset() {
    const sel = window.getSelection();
    if (!sel?.rangeCount || !this.el.contains(sel.anchorNode)) return -1;
    const range = document.createRange();
    range.setStart(this.el, 0);
    range.setEnd(sel.anchorNode, sel.anchorOffset);
    let count = 0;
    const walker = document.createTreeWalker(range.cloneContents(), NodeFilter.SHOW_ALL);
    let node = walker.nextNode();
    while (node) {
      if (node.nodeType === Node.TEXT_NODE) count += node.textContent.replace(/\u200B/g, '').length;
      else if (node.nodeType === Node.ELEMENT_NODE && node.dataset?.path) count += node.dataset.path.length + 4;
      node = walker.nextNode();
    }
    return count;
  }

  _restoreCaretCharOffset(offset) {
    if (offset < 0) return;
    try {
      let remaining = offset;
      const range = document.createRange();
      range.setStart(this.el, 0);
      range.collapse(true);
      const walk = (node) => {
        if (remaining < 0) return;
        if (node.nodeType === Node.TEXT_NODE) {
          const len = node.textContent.replace(/\u200B/g, '').length;
          if (remaining <= len) { range.setStart(node, remaining); range.collapse(true); remaining = -1; }
          else remaining -= len;
        } else if (node.nodeType === Node.ELEMENT_NODE && node.dataset?.path) {
          const len = node.dataset.path.length + 4;
          if (remaining <= len) { range.setStartAfter(node); range.collapse(true); remaining = -1; }
          else remaining -= len;
        } else { node.childNodes.forEach(walk); }
      };
      this.el.childNodes.forEach(walk);
      const sel = window.getSelection();
      sel.removeAllRanges();
      sel.addRange(range);
    } catch {}
  }
}


// ══════════════════════════════════════════════════════════════════════════════
// INTEGRATION LAYER  –  hooks into Alpine's action panel
// ══════════════════════════════════════════════════════════════════════════════

const badgeInputMap = new WeakMap();

const SKIP_FIELDS = new Set([
  'method', 'body_type', 'auth_type', 'is_html', 'mode',
  'cron_enabled', 'cron_schedule', 'form_id', 'timeout',
  'node_timeout', 'retry_count', 'retry_delay',
  'match_mode', 'match_count', 'delay_seconds', 'packages',
]);

function getAlpineData() {
  const root = document.querySelector('[x-data="zygo"]');
  return root ? Alpine.$data(root) : null;
}

function upgrade(el) {
  if (badgeInputMap.has(el)) return badgeInputMap.get(el);
  if (el.style.display === 'none') return null;
  if (SKIP_FIELDS.has(el.dataset.field || '')) return null;

  const raw = el.value || '';
  const isTextarea = el.tagName === 'TEXTAREA';
  const wrapper = document.createElement('div');
  wrapper.style.minHeight = isTextarea ? '72px' : '36px';
  el.parentNode.insertBefore(wrapper, el);
  el.style.display = 'none';

  let constructed = false;
  const bi = new BadgeInput(wrapper, raw, {
    placeholder: el.placeholder || '',
    onChange(newRaw) {
      if (!constructed) return;
      el.value = newRaw;
      el.dispatchEvent(new Event('input', { bubbles: true }));
    },
    getVariables() {
      const ad = getAlpineData();
      const nodeId = ad?.activeConfigNodeId;
      console.log('[getVariables] activeConfigNodeId:', nodeId);

      if (!ad || !nodeId) {
        console.warn('[getVariables] no Alpine data or no activeConfigNodeId');
        return [];
      }

      const vars = ad.getUpstreamVariables(nodeId);
      console.log('[getVariables] raw vars:', vars);

      const canvasData = ad.editor?.export()?.drawflow?.Home?.data || {};
      console.log('[getVariables] canvasData keys:', Object.keys(canvasData));
      console.log('[getVariables] latestRunOutputs:', ad.latestRunOutputs?.nodeOutputs);
      // Enrich each variable with a nodeLabel so the picker can group by
      // "nodeId nodeName" instead of just the raw path root (which is the
      // node label text and isn't unique when multiple nodes share a type).
      //
      // getUpstreamVariables builds paths as "{nodeLabel}.{key}", where
      // nodeLabel is the .node-card-title text. We recover the upstream node
      // IDs by scanning canvas connections and match them to path roots.
      // Build a map: pathRoot (node label text) → "id label" display string
      // We walk upstream the same way getUpstreamVariables does.
      const visited = new Set();
      const rootToDisplay = {};
      const walk = (nid) => {
        if (visited.has(nid)) return;
        visited.add(nid);
        Object.entries(canvasData).forEach(([srcId, srcNode]) => {
          Object.values(srcNode.outputs || {}).forEach(out => {
            (out.connections || []).forEach(conn => {
              if (String(conn.node) === String(nid)) {
                const titleEl = document.querySelector(`#node-${srcId} .node-card-title`);
                const label = titleEl?.textContent?.trim() || srcNode.name || srcId;
                // Display as "id label" e.g. "5 Transform"
                rootToDisplay[label] = `${srcId} ${label}`;
                walk(srcId);
              }
            });
          });
        });
      };
      walk(String(nodeId));

      return vars.map(v => {
        const root = v.path.split('.')[0];
        return { ...v, nodeLabel: rootToDisplay[root] || root };
      });
    },
  });

  constructed = true;
  badgeInputMap.set(el, bi);
  return bi;
}

function upgradeAll() {
  const body = document.getElementById('action-panel-body');
  if (!body) return;

  body.querySelectorAll('.cfg-input, .cfg-textarea').forEach(el => {
    if (!badgeInputMap.has(el) && el.style.display !== 'none') upgrade(el);
  });

  body.querySelectorAll('[id^="kv-"] .grid').forEach(row => {
    const inputs = row.querySelectorAll('input');
    if (inputs.length >= 2 && !badgeInputMap.has(inputs[1]) && inputs[1].style.display !== 'none') {
      upgrade(inputs[1]);
    }
  });
}

function watchPanel() {
  const body = document.getElementById('action-panel-body');
  if (!body) return;
  new MutationObserver(() => requestAnimationFrame(upgradeAll))
    .observe(body, { childList: true, subtree: true });
  upgradeAll();
}

function patchMethod(ad, name) {
  if (typeof ad[name] !== 'function') return;
  const orig = ad[name].bind(ad);
  ad[name] = function (...args) { orig(...args); setTimeout(upgradeAll, 80); };
}

document.addEventListener('DOMContentLoaded', () => {
  // Load operators from backend, then boot everything
  loadOperators().then(() => {
    watchPanel();
    const ad = getAlpineData();
    if (!ad) return;
    ['openConfigPanel', 'openIntegrationConfigPanel', 'refreshTransformConfig', '_renderConditionRules']
      .forEach(name => patchMethod(ad, name));
  });
});

window.ZygoBadges = { buildBadgeEl, BadgeInput, VariablePicker, picker, upgrade, upgradeAll, loadOperators };