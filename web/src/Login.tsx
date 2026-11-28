import { useState, type FormEvent } from 'react'
import { supabase } from './supabase'

export default function Login() {
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const entrar = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setError(null)
    const { error } = await supabase.auth.signInWithPassword({ email, password })
    if (error) setError('Correo o contraseña incorrectos.')
    setBusy(false)
  }

  return (
    <main className="login">
     <div className="folio">
      <div className="brand"><span className="marca" aria-hidden="true">MP</span><h1>Mesa de partes</h1></div>
      <p className="muted">Ingresa con la cuenta que te dio el equipo de datos.</p>
      <form onSubmit={entrar} className="stack">
        <label>
          Correo
          <input type="email" autoComplete="username" required value={email} onChange={(e) => setEmail(e.target.value)} />
        </label>
        <label>
          Contraseña
          <input type="password" autoComplete="current-password" required value={password} onChange={(e) => setPassword(e.target.value)} />
        </label>
        {error && <p role="alert" className="err aviso">{error}</p>}
        <button className="primary" disabled={busy}>{busy ? 'Ingresando…' : 'Ingresar'}</button>
      </form>
     </div>
    </main>
  )
}
