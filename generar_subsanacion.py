import pandas as pd
import os
from datetime import datetime
import pandas as pd
import sqlalchemy
from config import CON_URL

query = """SELECT EXPEDIENTE.ID_EXTERNO ID, TRA.REFERENCIA DESCRIPCION,
EXPEDIENTE.TIPO_DOCUMENTO||'-'||EXPEDIENTE.ANIO||'-'||TO_CHAR(EXPEDIENTE.NUMERO, 'FM00000000')||'-   -'
||EXPEDIENTE.CODIGO_REPARTICION_ACTUACION||'-'||EXPEDIENTE.CODIGO_REPARTICION_USUARIO
AS NUMERO_EE FROM EE_GED.EE_EXPEDIENTE_ELECTRONICO EXPEDIENTE
LEFT JOIN TAD2_GED.TAD_TRAMITE TRA ON TRA.ID = EXPEDIENTE.ID_EXTERNO
WHERE TRA.FECHA_ALTA > TO_DATE('01/01/2021','DD/MM/YYYY') AND TRA.NUMERO_EE IS NULL AND 
TRA.ID_TIPO_TRAMITE NOT IN (2,1368) AND EXPEDIENTE.CODIGO_REPARTICION_USUARIO = 'DLMJYDHGP'"""


def escribir_archivo(nom_archivo, seccion, *args):
    if seccion == 'encabezado':
        with open(nom_archivo, 'w+') as encabezado:
            encabezado.write(f"""
SET SERVEROUTPUT ON
SET DEFINE OFF

DECLARE
    v_registros_modificados NUMBER (30);
    v_numero_esperado       NUMBER (30);
    REG_ACT_EXCEPTION       EXCEPTION;

BEGIN
    v_numero_esperado       := {args[0]}; --Cantidad de updates en la base de datos
    v_registros_modificados := 0;
    DBMS_OUTPUT.put_line ('***COMIENZA SCRIPT***');
    DBMS_OUTPUT.put_line ('Se esperan modificar '||v_numero_esperado ||' registros');

--BEGIN UPDATE
""")

    if seccion == 'final':
        with open(nom_archivo, 'a') as final:
            final.write("""
--FIN UPDATE

IF (v_registros_modificados != v_numero_esperado) THEN
    RAISE REG_ACT_EXCEPTION;
END IF;

COMMIT; --!!!PARA EJECUTAR PASAR A COMMIT!!!
DBMS_OUTPUT.put_line ('Se modificaron '||v_registros_modificados||' registros');
DBMS_OUTPUT.put_line ('***COMMIT REALIZADO***');

EXCEPTION
WHEN REG_ACT_EXCEPTION THEN
BEGIN
    ROLLBACK;
    DBMS_OUTPUT.put_line ('SE REALIZA ROLLBACK DE TRANSACCION: ');
    DBMS_OUTPUT.put_line ('LA CANTIDAD DE REGISTROS INSERTADOS NO COINCIDE CON LA ESPERADA ' || v_registros_modificados);
END;

WHEN OTHERS THEN
BEGIN
    ROLLBACK;
    DBMS_OUTPUT.put_line ('SE REALIZA ROLLBACK DE TRANSACCION: ');
    DBMS_OUTPUT.put_line ('    ' || SUBSTR (SQLERRM,1, 200));
END;

END;""")

    if seccion == 'update':
        with open(nom_archivo, 'a') as update:
            update.write(f"""
-- {args[0]}
UPDATE TAD2_GED.TAD_TRAMITE SET NUMERO_EE='{args[1]}' WHERE ID = {args[2]}; 
v_registros_modificados := v_registros_modificados + SQL%%ROWCOUNT;
""")


if __name__ == '__main__':

    dt = datetime.now().strftime('%Y_%m_%d_%H_%M')
    nombre_archivo = f'PROD-RM1490-{dt}.sql'

    if os.path.exists(nombre_archivo):
        os.remove(nombre_archivo)

    engine = sqlalchemy.create_engine(CON_URL)
    with engine.connect() as conn:
        data = pd.read_sql(query, conn)
        print(data)

    escribir_archivo(nombre_archivo, 'encabezado', len(data))

    nro_esperado = len(data)
    for row in data.itertuples():
        id = row[1]
        descripcion = row[2]
        nro_expediente = row[3]
        if nro_expediente == '' or id == '':
            continue
        escribir_archivo(nombre_archivo, 'update',
                         descripcion, nro_expediente, id)

    escribir_archivo(nombre_archivo, 'final')