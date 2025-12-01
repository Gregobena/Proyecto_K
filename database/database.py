import sqlite3

class GestorPrincipal: 

    def __init__(self,db_file):
        try:
            self.conn = sqlite3.connect(db_file)
            print("Conexion Exitosa")
        except Exception as e: 
            print("Error en la conexion por:",e)
            self.conn = False 
        
    def crear_tablas(self): 
        try:
            if self.conn:
                cur = self.conn.cursor() 
                query = ''' 
                    CREATE TABLE Proveedor ( 
                        prov_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        telefono TEXT, 
                        nombre TEXT, 
                        email TEXT
                    );

                    CREATE TABLE Categoria (
                        cat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nombre TEXT UNIQUE
                    );

                    CREATE TABLE Prod ( 
                        prod_id INTEGER PRIMARY KEY AUTOINCREMENT, 
                        precio_venta REAL, 
                        nombre TEXT,
                        descripcion TEXT,  
                        cat_nom TEXT,
                        FOREIGN KEY (cat_nom) REFERENCES Categoria(nombre)
                    );

                    CREATE TABLE Venta (
                        venta_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fecha_hora DATETIME DEFAULT CURRENT_TIMESTAMP,  
                        ganancia REAL, 
                        costo_total REAL, 
                        ingreso_total REAL, 
                        pago TEXT
                    );

                    CREATE TABLE Compra (
                        compra_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        total REAL, 
                        pago TEXT,
                        prov_id INT DEFAULT NULL ,
                        FOREIGN KEY (prov_id) REFERENCES Proveedor(prov_id)
                    );

                    CREATE TABLE Detalle_Venta (
                        dv_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        prod_id INT,
                        venta_id INT,
                        cantidad INT,
                        precio_venta REAL, 
                        costo_total REAL,
                        FOREIGN KEY (prod_id) REFERENCES Prod(prod_id),
                        FOREIGN KEY (venta_id) REFERENCES Venta(venta_id)
                    );

                    CREATE TABLE Detalle_Compra (
                        dc_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        prod_id INT,
                        compra_id INT,
                        cantidad INT,
                        precio_unitario REAL, 
                        FOREIGN KEY (prod_id) REFERENCES Prod(prod_id),
                        FOREIGN KEY (compra_id) REFERENCES Compra(compra_id)
                    );

                    CREATE TABLE Lote ( 
                        lote_id INTEGER PRIMARY KEY AUTOINCREMENT, 
                        prod_id INT, 
                        fecha_hora DATETIME DEFAULT CURRENT_TIMESTAMP, 
                        stock INT, 
                        costo_unitario REAL, 
                        dc_id INT, 
                        FOREIGN KEY (dc_id) REFERENCES  Detalle_Compra(dc_id)
                    )

                '''
                cur.executescript(query)
                cur.close()
                print("Tablas creadas exitosamente")
                self.conn.commit()
            else:
                print("Error iniciando, no conectado a sqlite3")
        except Exception as e: 
            print("Error:",e)

    def set_productos(self,productos): 
        """
        Agrega una lista de productos a la DB, la lista debe ser de diccionarios con los siguientes valores
        Prod["precio_venta"], Prod["nombre"], Prod["descripcion"], Prod["cat_nom"]
        """
        try:
            if self.conn: 
                for prod in productos:
                    if 'cat_nom' in prod: 
                        prod["cat_nom"] = prod["cat_nom"].capitalize()      
                cur = self.conn.cursor()
                query = '''
                    INSERT INTO Prod (precio_venta, nombre, descripcion, cat_nom) 
                    VALUES (:precio_venta, :nombre, :descripcion, :cat_nom )
                '''
                cur.executemany(query, productos)
                self.conn.commit() 
                cur.close()
        except Exception as e: 
            print("Error:",e)

    def set_categorias(self,categorias): 
        """
        Agrega una lista de categorias a la DB, la lista debe ser de diccionarios con los siguientes valores
        Cat["nombre"]
        """

        try: 
            if self.conn: 
                cur = self.conn.cursor() 
                query = ''' 
                    INSERT INTO Categoria (nombre) VALUES (:nombre) 
                ''' 
                cur.executemany(query,categorias)
                self.conn.commit()
                cur.close()
        except Exception as e: 
            print("Error:",e)

    def get_categoria(self,item): 
        """
        Devuelve una categoria con todos sus atributos en el siguiente orden:
        (cat_id,nombre)
        el item puede ser tanto cat_id como nombre
        """
        try: 
            if self.conn:
                cur = self.conn.cursor() 
                if isinstance(item,int): 
                    query = '''
                    SELECT * FROM Categoria ca WHERE ca.cat_id = ? 
                    '''
                else: 
                    query = '''
                    SELECT * FROM Categoria ca WHERE ca.nombre = ?
                    '''
                cur.execute(query,(item,))
                return cur.fetchone()
        except Exception as e: 
            print("error:",e)
    def get_categorias(self,item): 
        """
        Devuelve las categorias con todos sus atributos en el siguiente orden:
        ((cat_id,nombre),..,(n))
        el item puede ser tanto [(item_id),..,(n)] como [(nombre),..,(n)]
        """
        try: 
            if self.conn:
                cur = self.conn.cursor() 
                lista = []
                for elem in item:
                    if isinstance(elem[0],int): 
                        query = '''
                        SELECT * FROM Categoria ca WHERE ca.cat_id = ? 
                        '''
                    else: 
                        query = '''
                        SELECT * FROM Categoria ca WHERE ca.nombre = ?
                        '''
                    cur.execute(query,elem)
                    lista.append(cur.fetchone())
                return lista 
        except Exception as e : 
            print("error:",e )

    def set_provedores(self,provedores): 
        """
        Agrega una lista de provedores a la DB, la lista debe ser de diccionarios con los siguientes valores
        Prov["nombre"], Prov["telefono"], Prov["email"]
        """

        try: 
            if self.conn: 
                cur = self.conn.cursor() 
                query = ''' 
                    INSERT INTO Proveedor (telefono,nombre,email) VALUES (:telefono, :nombre, :email) 
                ''' 
                cur.executemany(query,provedores)
                self.conn.commit()
                cur.close()
        except Exception as e: 
            print("Error:",e)
 
    def _Get_lotes(self,dv): # arreglar problema de que quede stock viejo por no registrar (error humano) proximamente
        try: 
            if self.conn:
                cur = self.conn.cursor()
                query = ''' 
                    SELECT l.lote_id,l.costo_unitario, l.stock,p.precio_venta
                    FROM Lote l JOIN Prod p on p.prod_id = l.prod_id
                    WHERE l.prod_id = ? and l.stock > 0 
                    ORDER BY l.fecha_hora DESC
                '''
                cur.execute(query,(dv["prod_id"],))
                lotes = cur.fetchall()
                return lotes 
        except Exception as e: 
            print("Error en buscar lotes:", e)

    def _Update_lotes(self,resultados): 
        try: 
            if self.conn:
                cur = self.conn.cursor()
                new_list = []
                for elem in resultados: 
                    new_list.append((elem[1],elem[0]))
                query = ''' 
                    UPDATE Lote SET stock = ? WHERE lote_id = ? 
                '''
                cur.executemany(query,new_list)
        except Exception as e: 
            print("error al actualizar los lotes: ",e)



    def set_venta(self,venta,pago="Sin Especificar"): # BUSCADOR  QUE TRADUCE LA INFO    
        """
        Agrega la venta al sistema, ingresa unicamente venta[i]["prod_id"] y venta[i]["cant"] i = {1,2,..,n}
        """ 
        try: 
            if self.conn: 
                cur = self.conn.cursor()
                ingreso_total,costo_total  =  0, 0
                lista_dv = [] 

                for dv in venta:
                    lotes = self._Get_lotes(dv) # lotes = [(l.id,l.costo_unitario, l.stock,p.precio_venta),..,(n)]
                    i, costo_total_dv = 0,0
                    resultados = []

                    # dv["cant"] = cantidad por vender 
                    cant = dv["cant"]
                    while cant > 0:
                        if i >= len(lotes): 
                            self.conn.rollback()
                            return (False,"LOTES INSUFICIENTES")

                        # si el stock < dv["cant"] entonces el stock restante es 0 
                        # sino es stock - dv["cant"]
                        lote_mayor = lotes[i][2] - cant
                        cant_stock = 0 if lote_mayor < 0 else lote_mayor

                        # si el stock > dv["cant"] entonces la cant_v es dv["cant"] (todo)
                        # sino es lo que habia en el lote 
                        aux = cant - lotes[i][2]
                        cant_vendida = cant if aux <= 0 else lotes[2] 

                        cant = aux 
                        resultados.append((lotes[i][0],cant_stock,lotes[i][1]))

                        costo_total_dv += cant_vendida * lotes[i][1] # * costo unitario
                        ingreso_total += cant_vendida * lotes[i][3] # * precio venta
                        i += 1  

                    self._Update_lotes(resultados)  
                    costo_total += costo_total_dv

                    lista_dv.append({
                        "prod_id" : dv["prod_id"],
                        "venta_id" : 0, 
                        "cant": dv["cant"],
                        "precio_venta": lotes[0][3],
                        "costo_total": costo_total_dv
                        })

                query = '''
                    INSERT INTO Venta (ingreso_total,costo_total,pago,ganancia)
                    VALUES (?, ?, ?, ?)
                '''
                cur.execute(query,(ingreso_total,costo_total,pago,ingreso_total - costo_total))
                id_venta = cur.lastrowid 
                for d in lista_dv: 
                    d["venta_id"] = id_venta

                query = '''
                    INSERT INTO Detalle_Venta (prod_id,venta_id,cantidad,precio_venta,costo_total)
                    VALUES (:prod_id, :venta_id, :cant, :precio_venta, :costo_total)
                '''

                cur.executemany(query,lista_dv)
                self.conn.commit()
        except Exception as e: 
            print("Error: ",e)
    def get_venta(self,venta_id): 

        try: 
            if self.conn:
                cur = self.conn.cursor() 
                query = '''     
                SELECT * FROM Venta v WHERE v.venta_id = ? 
                '''

                cur.execute(query,(venta_id,))
                return cur.fetchone()
        except Exception as e: 
            print("Error: ",e)
                        
    def set_compra(self,compras,pago="Sin Especificar",prov_id=None ): 
        """
        Agrega la compra al sistema, ingresa unicamente compra[i]["prod_id"] 
        , compra[i]["cant"] y compra[i]["precio"]
        i = {1,2,..,n}
        """ 

        try: 
            if self.conn : 
                cur = self.conn.cursor()
                total_costo = 0 
                lista_dc,lista_lotes = [],[]
                for dc in compras: 
                    lista_dc.append({
                        "prod_id" : dc["prod_id"], 
                        "cantidad" : dc["cant"],
                        "precio_unitario": dc["precio"],
                        "compra_id" : 0
                        })
                    total_costo += dc["precio"] * dc["cant"]
                query = '''
                    INSERT INTO Compra(prov_id,pago,total)
                    VALUES ( ?, ?, ?)
                '''
                cur.execute(query,(prov_id,pago,total_costo))
                id_compra = cur.lastrowid

                query = '''
                    INSERT INTO Detalle_Compra(prod_id,cantidad,precio_unitario,compra_id)
                    VALUES (:prod_id,:cantidad,:precio_unitario,:compra_id)
                '''

                for dc in lista_dc:
                    dc["compra_id"] = id_compra
                    cur.execute(query,dc)
                    dc_id = cur.lastrowid

                    lista_lotes.append({
                        "prod_id": dc["prod_id"],
                        "stock" : dc["cantidad"],
                        "costo_unitario" : dc["precio_unitario"],
                        "dc_id" : dc_id,
                        })
                query = '''
                    INSERT INTO Lote(prod_id,stock,costo_unitario,dc_id)
                    VALUES (:prod_id,:stock,:costo_unitario,:dc_id)
                '''
                cur.executemany(query,lista_lotes)
                self.conn.commit()
        except Exception as e: 
            print("Error:",e)
