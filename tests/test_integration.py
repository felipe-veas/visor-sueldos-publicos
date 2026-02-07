import os
import pandas as pd
import pytest
from public_salary_monitor.salary_analysis import process_file

TEST_FILE_PATH = "data/test_planta.csv"
EXPECTED_REPORT = "reporte_Presidencia_2024.xlsx"


@pytest.fixture
def setup_test_data():
    """Creates a dummy CSV file for testing and cleans up afterwards."""
    # Setup
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
    # Save with ; as separator and latin-1 which matches the application's expectation
    df.to_csv(TEST_FILE_PATH, sep=";", index=False, encoding="latin-1")

    yield

    # Teardown
    if os.path.exists(TEST_FILE_PATH):
        os.remove(TEST_FILE_PATH)
    if os.path.exists(EXPECTED_REPORT):
        os.remove(EXPECTED_REPORT)


def test_salary_processing_integration(setup_test_data):
    """
    Integration test ensuring the salary processing logic correctly generates a report
    given a known input file and filters.
    """
    # Act
    # We call the function that processes the file.
    # It catches exceptions internally, so if it fails, it prints to stdout.
    # We rely on the output file existence as the success criteria.
    process_file(TEST_FILE_PATH, org_filter="Presidencia", year_filter="2024")

    # Assert
    assert os.path.exists(EXPECTED_REPORT), (
        f"El archivo '{EXPECTED_REPORT}' no se generó. "
        "Revise los logs de salida para ver errores en process_file."
    )
