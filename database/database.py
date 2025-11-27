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
                        descripcion TEXT,  
                        cat_nom TEXT,
                        FOREIGN KEY (cat_nom) REFERENCES Categoria(nombre)
                    );

                    CREATE TABLE Venta (
                        venta_id INT PRIMARY KEY AUTOINCREMENT,
                        fecha_hora DATETIME DEFAULT CURRENT_TIMESTAMP,  
                        total REAL, 
                        forma_pago TEXT
                    );

                    CREATE TABLE Compra (
                        compra_id INT PRIMARY KEY AUTOINCREMENT,
                        total REAL, 
                        forma_pago TEXT,
                        prov_id INT,
                        FOREIGN KEY (prov_id) REFERENCES Proveedor(prov_id)
                    );

                    CREATE TABLE Detalle_Venta (
                        dv_id INT PRIMARY KEY AUTOINCREMENT,
                        prod_id INT,
                        venta_id INT,
                        cantidad INT,
                        precio_unitario REAL, 
                        costo_historico REAL,
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

                    CREATE TABLE Lotes ( 
                        lote_id INT PRIMARY KEY AUTOINCREMENT, 
                        prod_id INT, 
                        fecha_hora DATETIME DEFAULT CURRENT_TIMESTAMP, 
                        stock INT, 
                        costo_un REAL, 
                        dc_id INT, 
                        FOREIGN KEY (dc_id) REFERENCES  Detalle_Compra(dc_id)
                    )

                '''
                cur.executescript(query)
                cur.close()
                print("Tablas creadas exitosamente")
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

    def set_venta(self,venta): # BUSCADOR  QUE TRADUCE LA INFO    
        """
        Agrega la venta al sistema, ingresa unicamente venta[i]["prod_id"] y venta[i]["cant"] i = {1,2,..,n}
        """ 

        try: 
            if self.conn: 
                cur = self.conn.cursor()
                for dv in venta: # dv = detalle_venta 
                    query = '''
                    SELECT lot.costo_un, prod.precio_venta 
                    FROM Prod pr join Lote lot on (pr.prod_id = lot.prod_id)
                    Where pr.prod_id = ?
                    '''
                    cur.execute(query,(dv.prod_id))
                    costo, precio_venta = cur.fetchone()






