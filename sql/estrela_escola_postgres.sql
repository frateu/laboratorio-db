DROP SCHEMA IF EXISTS "escoladw" CASCADE;

CREATE SCHEMA IF NOT EXISTS "escoladw" ;

-- -----------------------------------------------------
-- Table `escoladw`.`DM_ALUNO`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escoladw"."DM_ALUNO" (
  "mat_alu" INT NOT NULL,
  "nome" VARCHAR(100) NOT NULL,
  "dat_entrada" DATE NOT NULL,
  "cotista" CHAR(1) NOT NULL,
  PRIMARY KEY ("mat_alu"));


-- -----------------------------------------------------
-- Table "escoladw"."DM_CURSO"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escoladw"."DM_CURSO" (
  "cod_curso" INT NOT NULL,
  "nom_curso" VARCHAR(80) NOT NULL,
  "nom_dpto" VARCHAR(50) NOT NULL,
  PRIMARY KEY ("cod_curso"));


-- -----------------------------------------------------
-- Table "escoladw"."DM_TEMPO"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escoladw"."DM_TEMPO" (
  "cod_tempo" INT NOT NULL,
  "ano" INT NOT NULL,
  "semestre" INT NOT NULL,
  PRIMARY KEY ("cod_tempo"));


-- -----------------------------------------------------
-- Table "escoladw"."DM_DISCIPLINA"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escoladw"."DM_DISCIPLINA" (
  "cod_disc" INT NOT NULL,
  "nom_disc" VARCHAR(60) NOT NULL,
  "carga_horaria" FLOAT NOT NULL,
  PRIMARY KEY ("cod_disc"));


-- -----------------------------------------------------
-- Table "escoladw"."DM_DESEMPENHO"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escoladw"."DM_DESEMPENHO" (
  "cod_desempenho" INT NOT NULL,
  "status" CHAR(2) NOT NULL,
  PRIMARY KEY ("cod_desempenho"));


-- -----------------------------------------------------
-- Table "escoladw"."FT_MATRICULA"
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "escoladw"."FT_MATRICULA" (
  "mat_alu" INT NOT NULL,
  "cod_tempo" INT NOT NULL,
  "cod_curso" INT NOT NULL,
  "cod_disc" INT NOT NULL,
  "cod_desempenho" INT NOT NULL,
  CONSTRAINT "mat_alu"
    FOREIGN KEY ("mat_alu")
    REFERENCES "escoladw"."DM_ALUNO" ("mat_alu")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT "cod_curso"
    FOREIGN KEY ("cod_curso")
    REFERENCES "escoladw"."DM_CURSO" ("cod_curso")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT "cod_tempo"
    FOREIGN KEY ("cod_tempo")
    REFERENCES "escoladw"."DM_TEMPO" ("cod_tempo")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT "cod_disc"
    FOREIGN KEY ("cod_disc")
    REFERENCES "escoladw"."DM_DISCIPLINA" ("cod_disc")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT "cod_desempenho"
    FOREIGN KEY ("cod_desempenho")
    REFERENCES "escoladw"."DM_DESEMPENHO" ("cod_desempenho")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
