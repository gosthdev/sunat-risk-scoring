import { createRoot } from 'react-dom/client'
import './styles.css'

const root = createRoot(document.getElementById('root')!)

if (!import.meta.env.VITE_SUPABASE_URL || !import.meta.env.VITE_SUPABASE_ANON_KEY) {
  root.render(
    <main className="page">
      <h1>Mesa de partes</h1>
      <p role="alert" className="err">Falta configurar VITE_SUPABASE_URL y VITE_SUPABASE_ANON_KEY en el entorno de despliegue.</p>
    </main>,
  )
} else {
  import('./App').then(({ default: App }) => root.render(<App />))
}
