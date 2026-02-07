from public_salary_monitor.salary_analysis import process_file
import os
import pandas as pd

# Path to the created test file
TEST_FILE_PATH = "data/test_planta.csv"


# Create test data
def create_test_data():
    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(
        {
            "organismo_nombre": ["Presidencia", "Ministerio X", "Presidencia"],
            "anyo": [2024, 2024, 2023],
            "Mes": ["Enero", "Febrero", "Marzo"],
            "Nombres": ["Juan", "Pedro", "Maria"],
            "Paterno": ["Perez", "Soto", "Gonzalez"],
            "Materno": ["Gomez", "Diaz", "Lopez"],
            "Sueldo Liquido": [1000000, 2000000, 1500000],
            "Tipo Estamento": ["Profesional", "Tecnico", "Administrativo"],
            "Tipo cargo": ["Analista", "Soporte", "Asistente"],
        }
    )
    # Save with ; as separator and latin-1 which is common in these files
    df.to_csv(TEST_FILE_PATH, sep=";", index=False, encoding="latin-1")
    print(f"Datos de prueba creados en {TEST_FILE_PATH}")


def clean_test_data():
    if os.path.exists(TEST_FILE_PATH):
        os.remove(TEST_FILE_PATH)
    expected_report = "reporte_Presidencia_2024.xlsx"
    if os.path.exists(expected_report):
        os.remove(expected_report)


print(">>> INICIANDO PRUEBA AUTOMATIZADA <<<")
create_test_data()
print(f"Archivo de prueba: {TEST_FILE_PATH}")

# Run the processor searching for 'Presidencia' in '2024'
process_file(TEST_FILE_PATH, org_filter="Presidencia", year_filter="2024")

# Verify if report was created
expected_report = "reporte_Presidencia_2024.xlsx"
if os.path.exists(expected_report):
    print(f"\n>>> PRUEBA EXITOSA: El archivo '{expected_report}' fue creado. <<<")
else:
    print(f"\n>>> FALLO: El archivo '{expected_report}' no se generó. <<<")

# Optional cleanup (comment to inspect results)
clean_test_data()
