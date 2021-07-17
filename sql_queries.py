# DROP TABLES

respondente_table_drop           = "DROP TABLE IF EXISTS respondente"
linguagem_programacao_table_drop = "DROP TABLE IF EXISTS linguagem_programacao"
resp_usa_linguagem_table_drop    = "DROP TABLE IF EXISTS resp_usa_linguagem"
resp_usa_ferramenta_table_drop   = "DROP TABLE IF EXISTS resp_usa_ferramenta"
ferramenta_comunic_table_drop    = "DROP TABLE IF EXISTS ferramenta_comunic"
sistema_operacional_table_drop   = "DROP TABLE IF EXISTS sistema_operacional"
pais_table_drop                  = "DROP TABLE IF EXISTS pais"
empresa_table_drop               = "DROP TABLE IF EXISTS empresa"

# CREATE TABLES

sistema_operacional_table_create = ("""
                                        CREATE TABLE IF NOT EXISTS sistema_operacional (
                                            id serial,
                                            nome varchar(255) NOT NULL,
                                            PRIMARY KEY(id)
                                        )
                                    """)

pais_table_create                = ("""
                                        CREATE TABLE IF NOT EXISTS pais (
                                            id serial,
                                            nome varchar(255) NOT NULL,
                                            PRIMARY KEY(id)
                                        )
                                    """)

empresa_table_create             = ("""
                                        CREATE TABLE IF NOT EXISTS empresa (
                                            id serial,
                                            tamanho varchar(255) NOT NULL,
                                            PRIMARY KEY(id)
                                        )
                                    """)

respondente_table_create         = ("""
                                        CREATE TABLE IF NOT EXISTS respondente (
                                            id serial, 
                                            nome varchar(255) NOT NULL, 
                                            contrib_open_source int, 
                                            programa_hobby int, 
                                            salario float NULL, 
                                            sistema_operacional_id int, 
                                            pais_id int, 
                                            empresa_id int,
                                            PRIMARY KEY(id),
                                            CONSTRAINT fk_sistema_operacional
                                                FOREIGN KEY(sistema_operacional_id)
                                                    REFERENCES sistema_operacional(id),
                                            CONSTRAINT fk_pais
                                                FOREIGN KEY(pais_id)
                                                    REFERENCES pais(id),
                                            CONSTRAINT fk_empresa
                                                FOREIGN KEY(empresa_id)
                                                    REFERENCES empresa(id)
                                        ) 
                                    """)

linguagem_programacao_create     = ("""
                                        CREATE TABLE IF NOT EXISTS linguagem_programacao (
                                            id serial,
                                            nome varchar(255) NOT NULL,
                                            PRIMARY KEY(id)
                                        )
                                    """)

resp_usa_linguagem_table_create  = ("""
                                        CREATE TABLE IF NOT EXISTS resp_usa_linguagem (
                                            respondente_id int,
                                            linguagem_programacao_id int,
                                            CONSTRAINT fk_respondente
                                                FOREIGN KEY(respondente_id)
                                                    REFERENCES respondente(id),
                                            CONSTRAINT fk_linguagem_programacao
                                                FOREIGN KEY(linguagem_programacao_id)
                                                    REFERENCES linguagem_programacao(id)
                                        )
                                    """)

ferramenta_comunic_table_create  = ("""
                                        CREATE TABLE IF NOT EXISTS ferramenta_comunic (
                                            id serial,
                                            nome varchar(255),
                                            PRIMARY KEY(id)
                                        )
                                    """)

resp_usa_ferramenta_table_create = ("""
                                        CREATE TABLE IF NOT EXISTS resp_usa_ferramenta (
                                            ferramenta_comunic_id int,
                                            respondente_id int,
                                            CONSTRAINT fk_ferramenta_comunic
                                                FOREIGN KEY(ferramenta_comunic_id)
                                                    REFERENCES ferramenta_comunic(id),
                                            CONSTRAINT fk_respondente
                                                FOREIGN KEY(respondente_id)
                                                    REFERENCES respondente(id)
                                        )
                                    """)



# INSERT RECORDS

sistema_operacional_table_insert   = ("""
                                        INSERT INTO sistema_operacional 
                                            (nome) 
                                        VALUES 
                                            (%s) 
                                    """)

pais_table_insert                  = ("""
                                        INSERT INTO pais 
                                            (nome) 
                                        VALUES 
                                            (%s) 
                                    """)

empresa_table_insert               = ("""
                                        INSERT INTO empresa 
                                            (tamanho) 
                                        VALUES 
                                            (%s) 
                                    """)

respondente_table_insert           = ("""
                                        INSERT INTO respondente 
                                            ( 
                                                nome, 
                                                contrib_open_source, 
                                                programa_hobby, 
                                                salario, 
                                                sistema_operacional_id, 
                                                pais_id, 
                                                empresa_id
                                            ) 
                                        VALUES 
                                            (%s, %s, %s, %s, %s, %s, %s);
                                        """)
linguagem_programacao_table_insert = ("""
                                        INSERT INTO linguagem_programacao 
                                            (nome) 
                                        VALUES 
                                            (%s) 
                                    """)

resp_usa_linguagem_table_insert    = ("""
                                        INSERT INTO resp_usa_linguagem
                                            (respondente_id, linguagem_programacao_id) 
                                        VALUES 
                                            (%s, %s) 
                                    """)

ferramenta_comunic_table_insert    = ("""
                                        INSERT INTO ferramenta_comunic
                                            (nome) 
                                        VALUES 
                                            (%s)  
                                    """)

resp_usa_ferramenta_table_insert   = ("""
                                        INSERT INTO resp_usa_ferramenta
                                            (ferramenta_comunic_id, respondente_id) 
                                        VALUES 
                                            (%s, %s)  
                                    """)

# QUERY LISTS

create_table_queries = ([
                            sistema_operacional_table_create,
                            pais_table_create,
                            empresa_table_create,
                            respondente_table_create,
                            linguagem_programacao_create,
                            resp_usa_linguagem_table_create,
                            ferramenta_comunic_table_create,
                            resp_usa_ferramenta_table_create
                        ])
drop_table_queries   = ([
                            resp_usa_linguagem_table_drop, 
                            resp_usa_ferramenta_table_drop,
                            respondente_table_drop, 
                            linguagem_programacao_table_drop, 
                            ferramenta_comunic_table_drop, 
                            sistema_operacional_table_drop, 
                            pais_table_drop,empresa_table_drop 
                       ])