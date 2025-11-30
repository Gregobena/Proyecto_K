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
                        prov_id INT PRIMARY KEY AUTOINCREMENT,
                        telefono TEXT, 
                        nombre TEXT, 
                        email TEXT
                    );

                    CREATE TABLE Categoria (
                        cat_id INT PRIMARY KEY,
                        nombre TEXT
                    );

                    CREATE TABLE Prod ( 
                        prod_id INT PRIMARY KEY AUTOINCREMENT, 
                        precio_venta REAL, 
                        nombre TEXT,
                        desc TEXT,  
                        cat_nom TEXT,
                        FOREIGN KEY (cat_nom) REFERENCES Categoria(nombre)
                    );

                    CREATE TABLE Venta (
                        venta_id INT PRIMARY KEY AUTOINCREMENT,
                        fecha_hora DATETIME DEFAULT CURRENT_TIMESTAMP,  
                        ganancia REAL, 
                        costo_total REAL, 
                        ingreso_total REAL, 
                        pago TEXT,
                    );

                    CREATE TABLE Compra (
                        compra_id INT PRIMARY KEY AUTOINCREMENT,
                        total REAL, 
                        pago TEXT,
                        prov_id INT,
                        FOREIGN KEY (prov_id) REFERENCES Proveedor(prov_id)
                    );

                    CREATE TABLE Detalle_Venta (
                        dv_id INT PRIMARY KEY AUTOINCREMENT,
                        prod_id INT,
                        venta_id INT,
                        cantidad INT,
                        precio_unitario REAL, 
                        costo_unitario REAL,
                        FOREIGN KEY (prod_id) REFERENCES Prod(prod_id),
                        FOREIGN KEY (venta_id) REFERENCES Venta(venta_id)
                    );

                    CREATE TABLE Detalle_Compra (
                        dc_id INT PRIMARY KEY AUTOINCREMENT,
                        prod_id INT,
                        compra_id INT,
                        cantidad INT,
                        precio_unitario REAL, 
                        FOREIGN KEY (prod_id) REFERENCES Prod(prod_id),
                        FOREIGN KEY (Compra_id) REFERENCES Compra(Compra_id)
                    );

                    CREATE TABLE Lote ( 
                        lote_id INT PRIMARY KEY AUTOINCREMENT, 
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
                conn.commit()
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
                        prod[cat_nom] = prod[cat_nom].capitalize()      
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
                cur.close()
        except Exception as e: 
            print("Error:",e)

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
                cur.executemany(query,categorias)
                cur.close()
        except Exception as e: 
            print("Error:",e)
 
    def get_lotes(self,dv): # arreglar problema de que quede stock viejo por no registrar (error humano) proximamente
        try: 
            if self.conn:
                cur = self.conn.cursor()
                query = ''' 
                    SELECT l.id,l.costo_unitario, l.stock,p.precio_venta
                    FROM Lote l JOIN Prod p on p.prod_id = l.prod_id
                    WHERE l.prod_id = ? and l.stock > 0 
                    ORDER BY l.fecha_hora DESC
                '''
                cur.execute(query,(dv["prod_id"]))
                lotes = cur.fetchall()
                return lotes 
        except Exception as e: 
            print("Error en buscar lotes:", e)

    def update_lotes(self,resultados): 
        for elem in resultados: 
            query = ''' 
            UPDATE Lote SET stock = ? WHERE lote_id = 
            '''



    def set_venta(self,venta,pago=None): # BUSCADOR  QUE TRADUCE LA INFO    
        """
        Agrega la venta al sistema, ingresa unicamente venta[i]["prod_id"] y venta[i]["cant"] i = {1,2,..,n}
        """ 
        try: 
            if self.conn: 
                cur = self.conn.cursor()
                ingreso_total,costo_total  =  0, 0 

                for dv in venta:
                    lotes = get_lotes(dv) 
                    # lotes = [(l.id,l.costo_unitario, l.stock,p.precio_venta),..,(n)]
                    i = 0
                    while dv["cant"] > 0:
                        # dv["cant"] = cantidad por vender 

                        # si el stock < dv["cant"] entonces el stock restante es 0 
                        # sino es stock - dv["cant"]
                        lote_mayor = lotes[i][2] - dv["cant"]
                        cant_stock = 0 if lote_mayor < 0 else lote_mayor

                        # si el stock > dv["cant"] entonces la cant_v es dv["cant"] (todo)
                        # sino es lo que habia en el lote 
                        aux = dv["cant"] - lotes[i][2]
                        cant_vendida = dv["cant"] if aux <= 0 else lote[2] 

                        dv["cant"] = aux 
                        resultados.append((lotes[i][0],lotes[i][1],cant_vendida,cant_stock))
                        i += 1 


 


                        # OBTENER VENTA CON RESULTADOS 
                    for elem in resultados: 
                        costo_total += elem[3] * elem[1] # cant_v * 
                        ingreso_total += lotes[0][3] * elem[1] 


                    for elem in resultados: 
                        query = '''
                            INSERT INTO Detalle_venta(prod_id,venta_id,cantidad,precio_unitario,costo_unitario)
                            VALUES (:prod_id,:venta_id,:cantidad,:precio_unitario,:costo_unitario)
                        '''