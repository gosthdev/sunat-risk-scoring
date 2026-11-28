// Puro (sin APIs de Deno): lo importan las edge functions y la web.
const MESES = ["ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO", "JULIO", "AGOSTO", "SETIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"];

/** "YYYY-MM" deducido del nombre (YYYYMM o MES+YYYY); null si no hay o es ambiguo. */
export function deducirPeriodo(nombre: string): string | null {
  const n = nombre.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase().replaceAll("SEPTIEMBRE", "SETIEMBRE");
  const hallados = new Set<string>();
  const maxAnio = new Date().getUTCFullYear() + 1;
  const add = (anio: string, mes: string) => { if (+anio >= 2000 && +anio <= maxAnio) hallados.add(`${anio}-${mes}`); };
  // sin lookbehind (Safari < 16.4): (^|\D) hace de borde izquierdo
  for (const m of n.matchAll(/(^|\D)(\d{4})(0[1-9]|1[0-2])(?!\d)/g)) add(m[2], m[3]);
  for (const m of n.matchAll(new RegExp(`(${MESES.join("|")})[^A-Z0-9]?(\\d{4})(?!\\d)`, "g"))) {
    add(m[2], String(MESES.indexOf(m[1]) + 1).padStart(2, "0"));
  }
  return hallados.size === 1 ? [...hallados][0] : null;
}
