import { createClient, FunctionsHttpError } from '@supabase/supabase-js'

export const supabase = createClient(
  import.meta.env.VITE_SUPABASE_URL,
  import.meta.env.VITE_SUPABASE_ANON_KEY,
)

export type Estado =
  | 'firmada' | 'error_subida' | 'subida' | 'encolada' | 'intake' | 'pendientes'
  | 'bronze' | 'silver' | 'completada' | 'rechazada' | 'duplicada' | 'error'

export interface Carga {
  id: string
  lote_id: string
  fuente: string
  periodo: string
  archivo: string
  tamano: number
  estado: Estado
  detalle_error: string | null
  github_run_url: string | null
  creado_en: string
  actualizado_en?: string
}

export interface CargaFirmada {
  carga_id: string
  nombre: string
  url: string
  fields: Record<string, string>
}

export class ApiError extends Error {
  constructor(message: string, readonly status?: number, readonly cuerpo?: { omitidos?: string[] } & Record<string, unknown>) { super(message) }
}

export async function invocar<T>(fn: string, body: Record<string, unknown>): Promise<T> {
  const { data, error } = await supabase.functions.invoke(fn, { body })
  if (error) {
    if (error instanceof FunctionsHttpError) {
      let cuerpo: ApiError['cuerpo']
      try { cuerpo = await error.context.json() } catch { /* cuerpo no JSON */ }
      throw new ApiError(typeof cuerpo?.error === 'string' ? cuerpo.error : error.message, error.context.status, cuerpo)
    }
    throw new ApiError(error.message)
  }
  return data as T
}
