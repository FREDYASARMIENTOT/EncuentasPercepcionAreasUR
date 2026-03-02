import os

# ==========================================================
# CONFIGURACIÓN DE SEGURIDAD
# ==========================================================
SECRETO_REAL = "TU_LLAVE_DE_AZURE_REAL_AQUI" # ⚠️ PON TU LLAVE REAL AQUÍ
PLACEHOLDER = "TU_SECRET_AQUI"

DIRECTORIOS = ['./', './VersionesEncuestas']
EXTENSIONES = ['.py', '.ipynb', '.sql', '.bat', '.txt']
# Lista de codificaciones a intentar (Windows suele usar latin-1 para SQL)
ENCODINGS = ['utf-8', 'latin-1', 'cp1252', 'utf-16']

def procesar_archivos(modo="OCULTAR"):
    buscar = SECRETO_REAL if modo == "OCULTAR" else PLACEHOLDER
    reemplazar = PLACEHOLDER if modo == "OCULTAR" else SECRETO_REAL
    
    print(f"--- MODO: {modo} ---")
    for directorio in DIRECTORIOS:
        for root, dirs, files in os.walk(directorio):
            if '.git' in root or '.conda' in root: continue
            
            for file in files:
                # No procesar el propio script de sanitización
                if file == "sanitizer.py": continue
                
                if any(file.endswith(ext) for ext in EXTENSIONES):
                    path = os.path.join(root, file)
                    contenido = None
                    encoding_usado = None
                    
                    # Intento de lectura con diferentes encodings
                    for enc in ENCODINGS:
                        try:
                            with open(path, 'r', encoding=enc) as f:
                                contenido = f.read()
                            encoding_usado = enc
                            break 
                        except:
                            continue
                    
                    if contenido and buscar in contenido:
                        try:
                            nuevo_contenido = contenido.replace(buscar, reemplazar)
                            with open(path, 'w', encoding=encoding_usado) as f:
                                f.write(nuevo_contenido)
                            print(f"✅ Procesado ({encoding_usado}): {path}")
                        except Exception as e:
                            print(f"❌ Error escribiendo {path}: {e}")

if __name__ == "__main__":
    accion = input("¿Qué quieres hacer? (1: OCULTAR para Git / 2: MOSTRAR para Producción): ")
    if accion == "1": procesar_archivos("OCULTAR")
    elif accion == "2": procesar_archivos("MOSTRAR")