// Existing location depth 01 remains nearest the door; never renumber stored stock.
export function containerLayout(containerNo, mode, flip) {
  const n = Number(containerNo);
  if (!Number.isInteger(n) || n < 1 || n > 7) throw new Error('Unknown container');
  const short = n === 1;
  const normal = short ? '8-slot' : '18-slot';
  // Old server versions wrote S/L and 0/1 instead of the UI values.
  mode = !mode || mode === 'S' ? normal : mode === 'L' ? (short ? '9-slot' : '20-slot') : mode;
  flip = flip === '1' || flip === 'R_LONG' ? 'R_LONG' : 'L_LONG';
  if (![normal, short ? '9-slot' : '20-slot'].includes(mode)) throw new Error(`Invalid layout for C${n}`);
  const base = short ? 4 : 9;
  const long = short ? 5 : 11;
  const extended = mode !== normal;
  const leftMax = extended && flip === 'L_LONG' ? long : base;
  const rightMax = extended && flip === 'R_LONG' ? long : base;
  return { mode, flip, leftMax, rightMax, label: `${leftMax + rightMax} pallets (${leftMax} left / ${rightMax} right)` };
}

export function slotKey(containerNo, side, depth) {
  return `C${Number(containerNo)}-${side}-${String(depth).padStart(2, '0')}`;
}
