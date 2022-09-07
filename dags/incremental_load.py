from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import psycopg2.extras
import random
from random import randint
import pandas as pd
from select import select
import csv
import logging

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

# Gerar novos inserts de alunos no banco de dados
def create_new_insert():
    conn = connect_db()
    cur = conn.cursor()
    
    nomeCSV = pd.read_csv('/opt/airflow/data/nomes.csv')

    cur.execute("SELECT * FROM escola.cursos")
    cursos = cur.fetchall()

    cur.execute("SELECT * FROM escola.alunos")
    alunos = cur.fetchall()
    
    cur.execute("SELECT * FROM escola.disciplinas")
    disciplinas = cur.fetchall()
    
    count_insert = randint(0, 100)
    date_insert = datetime.today().strftime('%Y-%m-%d')
    cotistaData = {1:"S",2:"N"}

    for i in range(len(alunos), len(alunos) + count_insert):
        nome = nomeCSV.iloc[randint(0, len(nomeCSV))]['group_name']
        cotista = cotistaData[randint(1, 2)]
        curso = cursos[randint(0, len(cursos) - 1)][0]
        semestre = str(randint(1, 2))
        ano = str(randint(2017, 2022))
        disciplina = disciplinas[randint(0, len(disciplinas) - 1)][0]
        nota = round(random.uniform(0, 10), 1)
        faltas = randint(0, 60)

        cur.execute('SELECT COUNT(cod_tempo) FROM escoladw."DM_TEMPO" WHERE cod_tempo = {}'.format((ano + semestre)))
        qtd_cod_tempo = cur.fetchone()

        if int(qtd_cod_tempo[0]) == 0:
            cur.execute('INSERT INTO escoladw."DM_TEMPO" VALUES ({}, {}, {})'.format((ano + semestre), ano, semestre))

        cur.execute("INSERT INTO escola.alunos VALUES ({}, '{}', '{}', '{}', {});".format((i + 1), nome, date_insert, cotista, curso))
        cur.execute("INSERT INTO escola.matriculas VALUES ({}, {}, {}, {}, {}, '{}');".format((ano + semestre) , (i + 1), disciplina, nota, faltas, ''))
    conn.commit()
    
# Extração dos dados do banco     
def extract_and_transform_data():
    conn = connect_db()
    cur = conn.cursor()
    
    qtd_new_alunos = 0
    qtd_new_cursos = 0
    qtd_new_disciplinas = 0
        
    qtd_controle = pd.read_csv('/opt/airflow/data/controle_escola.csv', header = None, sep = ',')
    
    # Incremental Alunos
    cur.execute('SELECT COUNT(mat_alu) FROM escola.alunos')
    count_alunos = cur.fetchone()
    qtd_alunos = int(count_alunos[0])
    
    if int(qtd_controle[0][1]) != int(count_alunos[0]):
        cur.execute('SELECT * FROM escola.alunos WHERE mat_alu between {0} and {1}'.format((int(qtd_controle[0][1]) + 1), count_alunos[0]))
        new_alunos = cur.fetchall()
        qtd_new_alunos = int(count_alunos[0]) - int(qtd_controle[0][1])
        
        with open('/opt/airflow/csv/DM_ALUNO.csv', 'a') as file:
            writer = csv.writer(file)
            
            for aluno in new_alunos:
                aluno_data = [aluno[0], aluno[1], aluno[2], aluno[3]]
                writer.writerow(aluno_data)
                
    # Incremental Cursos
    cur.execute('SELECT COUNT(cod_curso) FROM escola.cursos')
    count_cursos = cur.fetchone()
    qtd_cursos = int(count_cursos[0])
    
    if int(qtd_controle[2][1]) != int(count_cursos[0]):
        cur.execute('SELECT * FROM escola.cursos WHERE cod_curso between {0} and {1}'.format((int(qtd_controle[2][1]) + 1), count_cursos[0]))
        new_cursos = cur.fetchall()
        qtd_new_cursos = int(count_cursos[0]) - int(qtd_controle[2][1])
        
        with open('/opt/airflow/csv/DM_CURSO.csv', 'a') as file:
            writer = csv.writer(file)
            
            for curso in new_cursos:
                cur.execute('SELECT nome_dpto FROM escola.departamentos WHERE cod_dpto = {0}'.format(curso[2]))
                nome_dpto = cur.fetchone()
                
                curso_data = [curso[0], curso[1], nome_dpto[0]]
                writer.writerow(curso_data)
                
    # Incremental Cursos
    cur.execute('SELECT COUNT(cod_disc) FROM escola.disciplinas')
    count_disciplinas = cur.fetchone()
    qtd_disciplinas = int(count_disciplinas[0])
    
    if int(qtd_controle[1][1]) != int(count_disciplinas[0]):
        cur.execute('SELECT * FROM escola.disciplinas WHERE cod_disc between {0} and {1}'.format((int(qtd_controle[1][1]) + 1), count_disciplinas[0]))
        new_disciplinas = cur.fetchall()
        qtd_new_disciplinas = int(count_disciplinas[0]) - int(qtd_controle[1][1])
        
        with open('/opt/airflow/csv/DM_DISCIPLINA.csv', 'a') as file:
            writer = csv.writer(file)
            
            for disciplina in new_disciplinas:            
                disciplina_data = [disciplina[0], disciplina[1], disciplina[2]]
                writer.writerow(disciplina_data)
    
    # Incremental Matriculas
    cur.execute('SELECT COUNT(mat_alu) FROM escola.matriculas')
    count_matriculas = cur.fetchone()
    
    if int(qtd_controle[0][1]) != int(count_matriculas[0]):
        cur.execute('SELECT * FROM escola.matriculas WHERE mat_alu between {0} and {1}'.format((int(qtd_controle[0][1]) + 1), count_matriculas[0]))
        new_matriculas = cur.fetchall()
        
        with open('/opt/airflow/csv/FT_MATRICULA.csv', 'a') as file:
            writer = csv.writer(file)
            
            for matricula in new_matriculas:
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
                    
    controle_header = ['alunos', 'disciplinas', 'cursos']
    with open('/opt/airflow/data/controle_escola.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(controle_header)
        writer.writerow([qtd_alunos, qtd_disciplinas, qtd_cursos])
        
    controle_new_header = ['new_alunos', 'new_disciplinas', 'new_cursos']
    with open('/opt/airflow/data/controle_new_escola.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(controle_new_header)
        writer.writerow([qtd_new_alunos, qtd_new_disciplinas, qtd_new_cursos])
    
def load_new_data():
    conn = connect_db()
    cur = conn.cursor()
    
    qtd_controle = pd.read_csv('/opt/airflow/data/controle_new_escola.csv', header = None, sep = ',')
    
    if int(qtd_controle[0][1]) > 0:
        # Load de DM_ALUNO
        with open('/opt/airflow/csv/DM_ALUNO.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader) # Pula o cabeçalho
            cur.execute('SELECT COUNT(mat_alu) FROM escola.alunos')
            count_alunos = cur.fetchone()
            qtd_alunos = int(count_alunos[0])
            for i in range(0, qtd_alunos - int(qtd_controle[0][1])):
                next(reader)
            for row in reader:
                cur.execute(
                'INSERT INTO escoladw."DM_ALUNO" VALUES (%s, %s, %s, %s)',
                row
            )
        conn.commit()
    
    if int(qtd_controle[2][1]) > 0:
        # Load de DM_CURSO
        with open('/opt/airflow/csv/DM_CURSO.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader) # Pula o cabeçalho
            cur.execute('SELECT COUNT(cod_curso) FROM escola.cursos')
            count_cursos = cur.fetchone()
            qtd_cursos = int(count_cursos[0])
            for i in range(0, qtd_cursos - int(qtd_controle[2][1])):
                next(reader)
            for row in reader:
                cur.execute(
                'INSERT INTO escoladw."DM_CURSO" VALUES (%s, %s, %s)',
                row
            )
        conn.commit()
    
    if int(qtd_controle[1][1]) > 0:
        # Load de DM_DISCIPLINA
        with open('/opt/airflow/csv/DM_DISCIPLINA.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader) # Pula o cabeçalho
            cur.execute('SELECT COUNT(cod_disc) FROM escola.disciplinas')
            count_disciplinas = cur.fetchone()
            qtd_disciplinas = int(count_disciplinas[0])
            for i in range(0, qtd_disciplinas - int(qtd_controle[1][1])):
                next(reader)
            for row in reader:
                cur.execute(
                'INSERT INTO escoladw."DM_DISCIPLINA" VALUES (%s, %s, %s)',
                row
            )
        conn.commit()
    
    if int(qtd_controle[0][1]) > 0:
        # Load de FT_MATRICULA
        with open('/opt/airflow/csv/FT_MATRICULA.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader) # Pula o cabeçalho
            cur.execute('SELECT COUNT(mat_alu) FROM escola.alunos')
            count_alunos = cur.fetchone()
            qtd_alunos = int(count_alunos[0])
            for i in range(0, qtd_alunos - int(qtd_controle[0][1])):
                next(reader)
            for row in reader:
                cur.execute(
                'INSERT INTO escoladw."FT_MATRICULA" VALUES (%s, %s, %s, %s, %s)',
                row
            )
        conn.commit()
    
# Criação e execução da DAG
with DAG(
        dag_id="incremental_load",
        schedule_interval='*/2 * * * *',
        default_args={
            "owner": "Matheus de Freitas Anjos",
            "retries": 1,
            "retry_delay": timedelta(minutes=1),
            "start_date": datetime(2022, 1, 1),
        },
        catchup=False
    ) as dag:
        
    insert_new_data = PythonOperator(
        task_id="insert_new_data",
        python_callable=create_new_insert
    )
    
    extract_transformm_new_data = PythonOperator(
        task_id="extract_transformm_new_data",
        python_callable=extract_and_transform_data
    )
    
    load_all_new_data = PythonOperator(
        task_id="load_all_new_data",
        python_callable=load_new_data
    )
    
    insert_new_data >> extract_transformm_new_data >> load_all_new_data