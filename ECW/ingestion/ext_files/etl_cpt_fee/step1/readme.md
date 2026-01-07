-- Step 1

-actualzia el aux.cpt_fee
-este solo ejecuta con insumo xlsx
-Requeire libreria OpenXlsx

--table sql
CREATE TABLE aux.cpt_fee
(
    id_cpt_fee bigint NOT NULL,
	cpt_description character varying(1000),
    cpt_code character varying(50),
    cpt_group character varying(500),
    mod_1 character varying(500),
    mod_2 character varying(500),
    resource_provider_type character varying(50),
    fee_plan character varying(500),
    fee real,
    id_fee_plan bigint,
    PRIMARY KEY (id_cpt_fee)
);

ALTER TABLE IF EXISTS aux.cpt_fee
    OWNER to postgres;