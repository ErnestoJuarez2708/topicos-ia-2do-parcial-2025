from tools import save_data_to_csv

# Datos simulados
data = [
    ("id", "name", "salary"),
    (1, "Alice", 3000),
    (2, "Bob", 2500),
    (3, "Charlie", 4000)
]

# Prueba
resultado = save_data_to_csv(data, "empleados_test.csv")
print(resultado)