export const fmtTamano = (b: number) =>
  b < 1024 ? `${b} B` : b < 1024 ** 2 ? `${(b / 1024).toFixed(1)} KB` : b < 1024 ** 3 ? `${(b / 1024 ** 2).toFixed(1)} MB` : `${(b / 1024 ** 3).toFixed(2)} GB`

export const fmtFecha = (iso: string) =>
  new Date(iso).toLocaleString('es-PE', { dateStyle: 'medium', timeStyle: 'short' })
