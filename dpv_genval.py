import csv
import pymysql
import sys
import os
import re
import json
import time
from datetime import datetime

class Constantes:
    @property
    def PI(self):  # aquí definimos la constante
        return 3.141592

constantes = Constantes()

class Files:
    def __init__(self,carpeta) -> None:
        lista = []
        self.carp=carpeta
        archivos = [archivo for archivo in os.listdir(carpeta) if os.path.isfile(os.path.join(carpeta, archivo))]
        part2 = "cuadromando\.inventario|cuadromando\.inventariodatos|cuadromando\.organismos|cuadromando\.penalizaciones|cuadromando\.penalizaciones_sla|cuadromando\.penalizaciones_slaobservaciones"
        part1 = "cuadromando\.volumetria|cuadromando\.economico|cuadromando\.economico_simulado|cuadromando\.inventario"
        p = re.compile("(cuadromando\.volumetria|cuadromando\.economico|cuadromando\.economico_simulado|cuadromando\.inventario|cuadromando\.inventario|cuadromando\.inventariodatos|cuadromando\.organismos|cuadromando\.penalizaciones|cuadromando\.penalizaciones_sla|cuadromando\.penalizaciones_slaobservaciones)\_([^T]{10}T[0-9_]{8})-([0-9]+)([FD]{1})?\.CSV")
        for archivo in sorted(archivos):
            r = p.match(archivo)
            if r and len(r.groups()) > 3 :
                lista.append(dict(nombre=r.groups(1)[0],fecha=r.groups(1)[1],posicion=r.groups(1)[2],clase= 'F' if str(r.groups(1)[3])=='1' else r.groups(1)[3] ,fichero=archivo)) 
        #self.files_csv = sorted(lista, key=lambda k: (k['nombre'],k['fecha'],k['posicion']),reverse=True)
        self.files_csv = sorted(lista, key=lambda k: (k['fecha'],k['nombre'],k['posicion']),reverse=True)

    #Selecciona los que tiene la fechamás reciente, para TODOS los CSV's        
    def borra_duplicados(self,clase)  -> None:
        if(len(self.files_csv)):
            lst = list(filter(lambda x: x['fecha'] ==  self.files_csv[0]['fecha'] and  x['clase'] == clase, self.files_csv))
            self.files_csv = sorted(lst, key=lambda k: (k['nombre'],k['fecha'],k['posicion']))   
    
    def lista_registros(self) -> list[dict]:
        reg = []
        for x in list(set([ x['nombre'] for x in self.files_csv ])): # emplea set() para eliminar duplicados!!!
            reg.append(dict(nombre=x,nreg=0))
            for e in list(filter(lambda n: n['nombre'] == reg[-1]['nombre'],self.files_csv)):
                with open(os.path.join( self.carp,e['fichero']),'r',encoding='utf-8') as archivo:
                    lin  = sum(1 for l in archivo)
                reg[-1]['nreg'] += lin - 1

        return reg
    
    def lista_registros_2(self) -> list[dict]:
        reg = []
        for x in list(set([ x['nombre'] for x in self.files_csv ])):                                            # nombres de tablas
            reg.append(dict(nombre=x,nreg=0))
            for e in list(filter(lambda n: n['nombre'] == reg[-1]['nombre'],self.files_csv)):                   # obtengo los archivos CSV  asociados a esa tabla
                with open(os.path.join(opc['basedir'],e['fichero']), newline='', encoding='UTF-8') as csvfile:
                    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
                    lin  = sum(1 for row in spamreader)
                reg[-1]['nreg'] += lin - 1
        
        return reg

    
class Tablas:
    dcon = None
    tablas = []
    _error = False

    def __init__(self,datos_conn,base,prefix) -> None:
        self.dcon = datos_conn; self._error = False
        conn = pymysql.connect(host=datos_conn["server"], user=datos_conn["user"], password=datos_conn["pass"])
        cnx = conn.cursor()
        try:
            cnx.execute(f"USE {base}");  cnx.execute("SHOW TABLES")
            lista = cnx.fetchall()
            lista = [ e[0] for e in lista if e[0].startswith(prefix) ]
            for x in lista:
                cnx.execute(f"DESCRIBE {base}.{x}")
                self.tablas.append(dict(nombre=x,campos=[ f"`{e[0]}`" for e in cnx.fetchall()  ]))

        except  Exception as e2:
            self._error = True 

        if(cnx):  cnx.close()
        if(conn): conn.close()


    def check_tablas(self,base,tablas,prefix) -> bool:
        error = True; ct = 0
        if self.dcon:
            conn = pymysql.connect(host=self.dcon["server"], user=self.dcon["user"], password=self.dcon["pass"])
            cnx = conn.cursor()
            try:
                cnx.execute(f"USE {base}");  cnx.execute(f"SHOW TABLES LIKE '{prefix}\_%'")
                lista = [ x[0] for x in cnx.fetchall()]
                cnx.execute(f"SHOW TABLES LIKE '{prefix}tmp\_%'")
                lista.extend( [ x[0] for x in cnx.fetchall()] )
                for x in tablas:
                   ct +=  1  if  x in lista and x.replace(f"{prefix}_",f"{prefix}tmp_") in lista else 0
                error = True if ct!=len(tablas) else False
            except  Exception as e2:
               pass
            if(cnx):  cnx.close()
            if(conn): conn.close()

        return error
    
    def truncar_tablas(self,base,prefix) -> bool:
        merror = None
        if self.dcon:
            conn = pymysql.connect(host=self.dcon["server"], user=self.dcon["user"], password=self.dcon["pass"])
            cnx = conn.cursor()
            try:
                cnx.execute(f"USE {base}");  cnx.execute(f"SHOW TABLES LIKE '{prefix}\_%'")
                for x in  cnx.fetchall():
                    cnx.execute(f"TRUNCATE TABLE {base}.{x[0]};")
                    conn.commit()
            except  Exception as e2:
                merror = f"Error en tabla  {x[0]}"

            if(cnx):  cnx.close()
            if(conn): conn.close()

        return merror
    
    def rename_tablas(self,base,prefix='genvaltmp_',prefix_new='genval1tmp_') -> bool:
        error = False
        if self.dcon:
            conn = pymysql.connect(host=self.dcon["server"], user=self.dcon["user"], password=self.dcon["pass"])
            cnx = conn.cursor()
            try:
                cnx.execute(f"USE {base}");  cnx.execute(f"SHOW TABLES LIKE '{prefix}\_%'")
                for x in  cnx.fetchall():
                    x2 = x[0].replace(prefix,prefix_new)
                    cnx.execute(f"RENAME TABLE {base}.{x[0]} TO {base}.{x2};")
                    conn.commit()
            except  Exception as e2:
                error = True 

            if(cnx):  cnx.close()
            if(conn): conn.close()

        return error

    def crear_sql(self,fichero,base,num,prefix='genvaltmp_'):
        Sql = f"REPLACE INTO {base}.{ [ x['nombre'].replace('cuadromando.',prefix) for x in self.tablas if x['nombre'] == fichero.replace('cuadromando.',prefix) ] [0]} "
        if self.dcon:
            str_campos = [ ','.join(x['campos']) for x in self.tablas if x['nombre'] == fichero.replace('cuadromando.',prefix) ]
            if(len(str_campos)):
                n_reg = str_campos[0].count(',')+1
                Sql  = f"{Sql}  ({str_campos[0]}) VALUES "
                cad ="%s,"*n_reg if(n_reg)  else ''
                if len(cad): 
                    cad = f"({cad[:len(cad)-1]} ),"
                cad = cad * num
                Sql = f"{Sql} {cad[:len(cad)-1]};"
        return Sql
    
    @property
    def berror(self) -> bool: return self._error
    @property
    def existe_tabla(self) -> bool: return self._existe

    def insert_row_genval(self,conn,cnx,r,sql,csv,base): 
        salida = {'error' : False, 'insert' : 0, "update" : 0,"id": None}
        #mitabla = [ x['nombre'].replace('cuadromando.','genval_') for x in self.tablas if x['nombre'] == csv.replace('cuadromando.','genval_') ] [0]
        rst = []
        for x in r:
            rst2 = list(map(lambda z:z  if(len(z)) else None, x))
            rst.extend(rst2)
        try:
            cnx.execute(sql,rst)
            conn.commit()
            salida['insert'] = len(r)  
        except  Exception as e2:
            salida['error'] =True  
        
        return salida


    def leo_csv_genval(self,csvf,opc,acum,log=sys.stdout) : # 192.168.1.104
        insert = acum; lin = 0; merror = None; bloque = 1000
        if self.dcon: # se ha probado la conexión y es ok!
            conn = pymysql.connect(host=self.dcon["server"], user=self.dcon["user"], password=self.dcon["pass"])
            cursor = conn.cursor()
            regs = []
            mitotal = list(filter(lambda e:csvf['nombre'] == e['nombre'], opc['totales']))[0]['nreg']
        

            with open(os.path.join(opc['basedir'],csvf['fichero']), newline='', encoding='UTF-8') as csvfile:
                spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
                header = next(spamreader); #print(header)
                for row in spamreader:
                    if lin % bloque or lin==0:
                        regs.append(row)
                    else:
                        regs.append(row)
                        sql = self.crear_sql(e['nombre'],opc['base'],len(regs))
                        rt = self.insert_row_genval(conn,cursor,regs,sql,csvf['nombre'],opc['base'])
                        if(rt['error']): 
                            merror = f'Fallo en la inserción en Linea {lin+1} del CSV.'
                            break
                        else:
                            insert += len(regs)
                            scad = f"'\033[1;36mFile: {csvf['fichero']}  {lin+1:0>6} -> ({insert:0>6} / {mitotal}) \033[0;m"
                            print("{: <100}".format(scad), end='\r') 
                        regs = []
                    lin += 1
            if len(regs) and  merror is None:
                sql = self.crear_sql(e['nombre'],opc['base'],len(regs))
                rt = self.insert_row_genval(conn,cursor,regs,sql,csvf['nombre'],opc['base'])
                if(rt['error']): 
                    merror = f'Fallo en la inserción en Linea {lin+1} del CSV.'
                else:
                    insert += len(regs)
                    scad = f"'\033[1;36mFile: {csvf['fichero']}  {lin:0>6} -> ({insert:0>6} / {mitotal}) \033[0;m"
                    print("{: <100}".format(scad), end='\r') 

            
            if(cursor):  cursor.close()
            if(conn):    conn.close()
        return (merror,insert)

        
if __name__=='__main__':     
    inicio = time.time()
    with open("opciones_genval.json", 'r') as json_file:
        opc = json.load(json_file)
    
    mifiles = Files(opc['basedir'])
    mifiles.borra_duplicados(opc['clase'])
    opc['totales'] = mifiles.lista_registros_2()
    oTablas = Tablas(opc['conexion'],opc['base'],'genvaltmp')

    #rt = oTablas.rename_tablas(opc['base'],'genvaltmp','genval1tmp')
    #rt = oTablas.rename_tablas(opc['base'],'genval','genvaltmp')
    #rt = oTablas.rename_tablas(opc['base'],'genval1tmp','genval')
    #rt = oTablas.crear_sql(mifiles.files_csv[0]['nombre'],opc['base'],3)

    with open(os.path.join(opc['basedir'],"genval.log"), 'w') as log_file: 
        if oTablas.check_tablas(opc['base'],opc['tablas'],'genval'):
            print(f"No se encontraron TODAS de GENVAL",file=log_file); log_file.flush()
            sys.exit(1) 

        rt = oTablas.truncar_tablas(opc['base'],'genvaltmp')
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - Encontrados {len(opc['totales'])} Tablas a recargar ...",file=log_file); log_file.flush() 
        completados = 0; merror = False
        for t in opc['totales']:
            acum = 0
            print(f"{t['nombre']} con {t['nreg']} Registros ...",file=log_file); log_file.flush()  
            for e in list(filter( lambda p: p['nombre'] == t['nombre'],mifiles.files_csv)):
                #if  e['nombre'] == 'cuadromando.volumetria':
                    (rt,acum) = oTablas.leo_csv_genval(e,opc,acum,log_file)
                    if rt is None: 
                        completados += 1
                        print(f"OK. Carga de {e['fichero']} con un total de {acum} registros.",file=log_file); log_file.flush() 
                        print("\nOK")
                    else: 
                        print(f"\nERROR:{rt} en fichero  {e['fichero']}\n",file=log_file); log_file.flush() 
                        merror = True; break
            if(merror):  break
        
        if(not merror):       
            if(completados == len(mifiles.files_csv)): 
                rt = oTablas.rename_tablas(opc['base'],'genvaltmp','genval1tmp')
                rt = oTablas.rename_tablas(opc['base'],'genval','genvaltmp')
                rt = oTablas.rename_tablas(opc['base'],'genval1tmp','genval')
                print(f"EJECUCIÓN (seg) = {time.time()-inicio}\n",file=log_file);  log_file.flush()   
                for x in mifiles.files_csv:
                    os.replace(os.path.join(opc['basedir'],x['fichero']),os.path.join(opc['basedir'],'OLD',x['fichero']))
                print('\nFIN EXITOSO. TRASLADO FICHEROS DE CARGA\n',file=log_file); log_file.flush() 
            else:
                  print(f"FALLO: Se han completado {completados} pero los csv a procesar eran {len(mifiles.files_csv)}\n",file=log_file);  log_file.flush()   






   