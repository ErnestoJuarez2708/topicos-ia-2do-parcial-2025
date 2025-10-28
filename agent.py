import dspy
import sqlite3
from dotenv import load_dotenv

from tools import execute_sql, get_schema, save_data_to_csv


# --- DSPy Agent Definition ---
class SQLAgentSignature(dspy.Signature):
    """
    Eres un agente inteligente especializado en transformar preguntas escritas en lenguaje natural en consultas SQL válidas.
    Tu objetivo es:
    1.- Comprender la intención del usuario expresada en lenguaje natural.
    2.- Usar las herramientas disponibles para examinar la estructura de la base de datos y ejecutar consultas SQL.
    3.- Analizar los resultados obtenidos y generar una respuesta clara y comprensible para el usuario.

    Herramientas disponibles:

    * execute_sql: Ejecuta consultas SQL directamente sobre la base de datos y devuelve los resultados.
    * get_schema: Obtiene la estructura de las tablas, incluyendo sus columnas y tipos de datos, para entender la base de datos.
    * save_data_to_csv: Guarda los resultados de las consultas en archivos CSV cuando el usuario lo solicite explícitamente.

    Reglas y restricciones:

    * Siempre revisa el esquema antes de formular una consulta si no estás seguro de los nombres de tablas o columnas.
    * Si una consulta genera un error, analiza el mensaje y corrígelo antes de volver a intentarlo.
    * Solo debes ejecutar consultas SELECT (no realices cambios con INSERT, UPDATE o DELETE).
    * Si el usuario pide guardar o exportar información, utiliza la herramienta save_data_to_csv.
    * Tus respuestas deben ser claras, concisas y en lenguaje natural, basadas en los resultados obtenidos.
    * Puedes realizar varios intentos para corregir errores, pero procura ser eficiente y preciso.
    """

    question = dspy.InputField(desc="The user's natural language question.")
    initial_schema = dspy.InputField(desc="The initial database schema to guide you.")
    answer = dspy.OutputField(
        desc="The final, natural language answer to the user's question."
    )


class SQLAgent(dspy.Module):
    """The SQL Agent Module"""
    def __init__(self, tools: list[dspy.Tool]):
        super().__init__()
        # Initialize the ReAct agent.
        self.agent = dspy.ReAct(
            SQLAgentSignature,
            tools=tools,
            max_iters=7,  # Set a max number of steps
        )

    def forward(self, question: str, initial_schema: str) -> dspy.Prediction:
        """The forward pass of the module."""
        result = self.agent(question=question, initial_schema=initial_schema)
        return result


def configure_llm():
    """Configures the DSPy language model."""
    load_dotenv()
    llm = dspy.LM(model="openai/gpt-4o-mini", max_tokens=4000)
    dspy.settings.configure(lm=llm)

    print("[Agent] DSPy configured with gpt-4o-mini model.")
    return llm


def create_agent(conn: sqlite3.Connection, query_history: list[str] | None = None) -> dspy.Module | None:
    if not configure_llm():
        return

    execute_sql_tool = dspy.Tool(
        name="execute_sql",
        desc="Ejecuta una consulta SQL directamente sobre la base de datos. Entrada: query (str) — una cadena que contiene una instrucción SQL válida. Salida: (str) — los resultados obtenidos de la consulta representados como texto, o un mensaje de error si la ejecución falla. Utiliza esta herramienta para obtener datos mediante consultas SELECT.",
        # Use lambda to pass the 'conn' object
        func=lambda query: execute_sql(conn, query, query_history),
    )

    get_schema_tool = dspy.Tool(
        name="get_schema",
        desc="Obtiene información estructural del esquema de la base de datos. Entrada: table_name (str o None) — si es None, devuelve la lista de todas las tablas disponibles; si se especifica una tabla, devuelve sus columnas y tipos de datos. Salida: (str) — una representación en texto con los nombres de las tablas o las columnas. Usa esta herramienta para explorar y comprender la estructura de la base de datos antes de realizar consultas.",
        # Use lambda to pass the 'conn' object
        func=lambda table_name: get_schema(conn, table_name),
    )

    save_csv_tool = dspy.Tool(
        name="save_data_to_csv",
        desc="Genera un archivo CSV con los resultados de una consulta cuando el usuario solicita guardar o exportar datos. Entrada: data (list[tuple]) — filas de datos que se desean guardar. filename (str, opcional) — nombre del archivo CSV de salida. query_description (str, opcional) — descripción adicional sobre los datos guardados. Salida: (str) — mensaje indicando si la operación fue exitosa y la ubicación del archivo creado. Nota: las consultas SELECT ya se guardan automáticamente en query_results.csv; usa esta herramienta solo cuando el usuario pida explícitamente exportar los resultados con un nombre específico.",
        func=save_data_to_csv
    )

    all_tools = [execute_sql_tool, get_schema_tool, save_csv_tool]

    # 2. Instantiate and run the agent
    agent = SQLAgent(tools=all_tools)

    return agent