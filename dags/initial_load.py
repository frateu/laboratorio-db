from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import psycopg2.extras
import csv

# Informações do banco postgres
DB_INFO = {
    'user':'postgres',
    'password':'postgrespw',
    'host':'172.18.0.3',
    'port':5432
}

# Conexão com o banco de dados
def connect_db():
    conn = psycopg2.connect(user=DB_INFO['user'],
                            password=DB_INFO['password'],
                            host=DB_INFO['host'],
                            port=DB_INFO['port'])
    return conn

# Inicialização do banco de dados
def create_db():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(open('/opt/airflow/sql/escola_postgres.sql', "r").read())
    conn.commit()
    cur.execute(open('/opt/airflow/sql/estrela_escola_postgres.sql', "r").read())
    conn.commit()

# Inserção da carga inicial
def insesrt_data():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(open('/opt/airflow/sql/inserts_escola_postgres.sql', "r").read())
    
    # Controle dos inserts
    cur.execute('SELECT COUNT(mat_alu) FROM escola.alunos')
    count_aluno = cur.fetchone()
    
    cur.execute('SELECT COUNT(cod_curso) FROM escola.cursos')
    count_curso = cur.fetchone()
    
    cur.execute('SELECT COUNT(cod_disc) FROM escola.disciplinas')
    count_disciplina = cur.fetchone()
    
    cur.execute('SELECT COUNT(cod_dpto) FROM escola.departamentos')
    count_departamento = cur.fetchone()
    
    controle_header = ['alunos', 'disciplinas', 'cursos']
    with open('/opt/airflow/data/controle_escola.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(controle_header)
        writer.writerow([count_aluno[0], count_disciplina[0], count_curso[0]])

# Extração dos dados do banco     
def extract_and_transform_data():
    conn = connect_db()
    cur = conn.cursor()
    
    # Extração e transformação do Aluno
    cur.execute('SELECT * FROM escola.alunos')
    alunos_extract = cur.fetchall()
    
    dm_aluno_header = ['mat_alu', 'nome', 'dat_entrada', 'cotista']
    with open('/opt/airflow/csv/DM_ALUNO.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(dm_aluno_header)
        
        for aluno in alunos_extract:
            aluno_data = [aluno[0], aluno[1], aluno[2], aluno[3]]
            writer.writerow(aluno_data)
    
    # Extração e transformação do Curso
    cur.execute('SELECT * FROM escola.cursos')
    cursos_extract = cur.fetchall()
    
    dm_curso_header = ['cod_curso', 'nom_curso', 'nom_dpto']
    with open('/opt/airflow/csv/DM_CURSO.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(dm_curso_header)
        
        for curso in cursos_extract:
            cur.execute('SELECT nome_dpto FROM escola.departamentos WHERE cod_dpto = {0}'.format(curso[2]))
            nome_dpto = cur.fetchone()
            
            curso_data = [curso[0], curso[1], nome_dpto[0]]
            writer.writerow(curso_data)
            
    # Extração e transformação de Disciplina
    cur.execute('SELECT * FROM escola.disciplinas')
    disciplinas_extract = cur.fetchall()
    
    dm_disciplina_header = ['cod_disc', 'nom_disc', 'carga_horaria']
    with open('/opt/airflow/csv/DM_DISCIPLINA.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(dm_disciplina_header)
        
        for disciplina in disciplinas_extract:            
            disciplina_data = [disciplina[0], disciplina[1], disciplina[2]]
            writer.writerow(disciplina_data)
            
    # CSV para DM_DESEMPENHO
    dm_desempenho_header = ['cod_desempenho', 'status']
    with open('/opt/airflow/csv/DM_DESEMPENHO.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(dm_desempenho_header)
        writer.writerow(['1', 'AP'])
        writer.writerow(['2', 'RM'])
        writer.writerow(['3', 'RF'])
        
    # Extração e transformação para DM_TEMPO
    cur.execute('SELECT semestre FROM escola.matriculas GROUP BY(semestre) ORDER BY semestre')
    semestre_extract = cur.fetchall()
    
    dm_tempo_header = ['cod_tempo', 'semestre']
    with open('/opt/airflow/csv/DM_TEMPO.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(dm_tempo_header)
        
        for semestre in semestre_extract:
            ano = int(float(int(semestre[0]) / 10))
            semestre_ano = int(semestre[0]) % 10

            writer.writerow([semestre[0], str(ano), str(semestre_ano)])
        
    # Extração e transformação para FT_MATRICULA
    cur.execute('SELECT * FROM escola.matriculas')
    matriculas_extract = cur.fetchall()
    
    ft_matricula_header = ['mat_alu', 'cod_tempo', 'cod_curso', 'cod_disc', 'cod_desempenho']
    with open('/opt/airflow/csv/FT_MATRICULA.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(ft_matricula_header)
        
        for matricula in matriculas_extract:
            cur.execute('SELECT cod_curso FROM escola.alunos WHERE mat_alu = {0}'.format(matricula[1]))
            cod_curso = cur.fetchone()
            
            cur.execute('SELECT carga_horaria FROM escola.disciplinas WHERE cod_disc = {0}'.format(matricula[2]))
            carga_horaria = cur.fetchone()
            
            if int(matricula[3]) < 7:
                matricula_data = [matricula[1], matricula[0], cod_curso[0], matricula[2], 2]
                writer.writerow(matricula_data)
            elif float(matricula[4]) > float(carga_horaria[0])/4:
                matricula_data = [matricula[1], matricula[0], cod_curso[0], matricula[2], 3]
                writer.writerow(matricula_data)
            else:
                matricula_data = [matricula[1], matricula[0], cod_curso[0], matricula[2], 1]
                writer.writerow(matricula_data)
            
def load_data():
    conn = connect_db()
    cur = conn.cursor()
    
    # Load de DM_ALUNO
    with open('/opt/airflow/csv/DM_ALUNO.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader) # Pula o cabeçalho
        for row in reader:
            cur.execute(
            'INSERT INTO escoladw."DM_ALUNO" VALUES (%s, %s, %s, %s)',
            row
        )
    conn.commit()
    
    # Load de DM_CURSO
    with open('/opt/airflow/csv/DM_CURSO.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader) # Pula o cabeçalho
        for row in reader:
            cur.execute(
            'INSERT INTO escoladw."DM_CURSO" VALUES (%s, %s, %s)',
            row
        )
    conn.commit()
    
    # Load de DM_DESEMPENHO
    with open('/opt/airflow/csv/DM_DESEMPENHO.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader) # Pula o cabeçalho
        for row in reader:
            cur.execute(
            'INSERT INTO escoladw."DM_DESEMPENHO" VALUES (%s, %s)',
            row
        )
    conn.commit()
    
    # Load de DM_DISCIPLINA
    with open('/opt/airflow/csv/DM_DISCIPLINA.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader) # Pula o cabeçalho
        for row in reader:
            cur.execute(
            'INSERT INTO escoladw."DM_DISCIPLINA" VALUES (%s, %s, %s)',
            row
        )
    conn.commit()
    
    # Load de DM_TEMPO
    with open('/opt/airflow/csv/DM_TEMPO.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader) # Pula o cabeçalho
        for row in reader:
            cur.execute(
            'INSERT INTO escoladw."DM_TEMPO" VALUES (%s, %s, %s)',
            row
        )
    conn.commit()
    
    # Load de FT_MATRICULA
    with open('/opt/airflow/csv/FT_MATRICULA.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader) # Pula o cabeçalho
        for row in reader:
            cur.execute(
            'INSERT INTO escoladw."FT_MATRICULA" VALUES (%s, %s, %s, %s, %s)',
            row
        )
    conn.commit()

# Criação e execução da DAG
with DAG(
        dag_id="initial_load",
        schedule_interval="@once",
        default_args={
            "owner": "Matheus de Freitas Anjos",
            "retries": 1,
            "retry_delay": timedelta(minutes=1),
            "start_date": datetime(2022, 1, 1),
        },
        catchup=False
    ) as dag:
    
    create_escola_db = PythonOperator(
        task_id="create_escola_db",
        python_callable=create_db
    )
        
    insert_all_data = PythonOperator(
        task_id="insert_all_data",
        python_callable=insesrt_data
    )
    
    extract_transformm_all_data = PythonOperator(
        task_id="extract_all_data",
        python_callable=extract_and_transform_data
    )
    
    load_all_data = PythonOperator(
        task_id="load_all_data",
        python_callable=load_data
    )
    
    create_escola_db >> insert_all_data >> extract_transformm_all_data >> load_all_data