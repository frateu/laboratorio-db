DROP SCHEMA IF EXISTS "escola" CASCADE;

CREATE SCHEMA IF NOT EXISTS "escola" ;

-- -----------------------------------------------------
-- Table `escola`.`alunos`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escola"."alunos" (
  "mat_alu" INT NOT NULL,
  "nome" VARCHAR(100) NOT NULL,
  "dat_entrada" DATE NOT NULL,
  "cotista" CHAR(1) NOT NULL,
  "cod_curso" INT NOT NULL);


-- -----------------------------------------------------
-- Table "escola"."cursos"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escola"."cursos" (
  "cod_curso" INT NOT NULL,
  "nom_curso" VARCHAR(80) NOT NULL,
  "cod_dpto" INT NOT NULL);


-- -----------------------------------------------------
-- Table "escola"."departamentos"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escola"."departamentos" (
  "cod_dpto"    INT NOT NULL,
  "nome_dpto"   VARCHAR(50) NOT NULL);


-- -----------------------------------------------------
-- Table "escola"."disciplinas"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escola"."disciplinas" (
  "cod_disc" INT NOT NULL,
  "nom_disc" VARCHAR(60) NOT NULL,
  "carga_horaria" FLOAT NOT NULL);


-- -----------------------------------------------------
-- Table "escola"."matriculas"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escola"."matriculas" (
    "semestre"   INT NOT NULL,
    "mat_alu"    INT NOT NULL,
    "cod_disc"   INT NOT NULL,
    "nota"       FLOAT,
    "faltas"     INT,
    "status"     CHAR(2));


-- -----------------------------------------------------
-- Table "escola"."matrizes_cursos"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escola"."matrizes_cursos" (
    "cod_curso"   INT NOT NULL,
    "cod_disc"    INT NOT NULL,
    "periodo"     INT NOT NULL);


ALTER TABLE "escola"."alunos" ADD CONSTRAINT "alu_fk" PRIMARY KEY ( "mat_alu" );


ALTER TABLE "escola"."cursos" ADD CONSTRAINT "cur_pk" PRIMARY KEY ( "cod_curso" );


ALTER TABLE "escola"."departamentos" ADD CONSTRAINT "departamentos_pk" PRIMARY KEY ( "cod_dpto" );


ALTER TABLE "escola"."disciplinas" ADD CONSTRAINT "disc_pk" PRIMARY KEY ( "cod_disc" );


ALTER TABLE "escola"."matriculas" ADD CONSTRAINT "mat_pk" PRIMARY KEY ( "mat_alu", "semestre" );


ALTER TABLE "escola"."matrizes_cursos" ADD CONSTRAINT "mcu_pk" PRIMARY KEY ( "cod_curso", "cod_disc" );

ALTER TABLE "escola"."alunos"
    ADD CONSTRAINT "alu_cur_fk" FOREIGN KEY ( "cod_curso" )
        REFERENCES "escola"."cursos" ( "cod_curso" );

ALTER TABLE "escola"."cursos"
    ADD CONSTRAINT "cur_der_fk" FOREIGN KEY ( "cod_dpto" )
        REFERENCES "escola"."departamentos" ( "cod_dpto" );

ALTER TABLE "escola"."matriculas"
    ADD CONSTRAINT "mat_alu_fk" FOREIGN KEY ( "mat_alu" )
        REFERENCES "escola"."alunos" ( "mat_alu" );

ALTER TABLE "escola"."matriculas"
    ADD CONSTRAINT "mat_dis_fk" FOREIGN KEY ( "cod_disc" )
        REFERENCES "escola"."disciplinas" ( "cod_disc" );

ALTER TABLE "escola"."matrizes_cursos"
    ADD CONSTRAINT "mcu_cur_fk" FOREIGN KEY ( "cod_curso" )
        REFERENCES "escola"."cursos" ( "cod_curso" );

ALTER TABLE "escola"."matrizes_cursos"
    ADD CONSTRAINT "mcu_dis_fk" FOREIGN KEY ( "cod_disc" )
        REFERENCES "escola"."disciplinas" ( "cod_disc" );
