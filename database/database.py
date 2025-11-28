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
                        ganancia REAL, 
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




    def set_venta(self,venta,pago=None): # BUSCADOR  QUE TRADUCE LA INFO    
        """
        Agrega la venta al sistema, ingresa unicamente venta[i]["prod_id"] y venta[i]["cant"] i = {1,2,..,n}
        """ 
        try: 
            if self.conn: 
                cur = self.conn.cursor()
                ingreso_total,costo_total  =  0, 0 

                for dv in venta:
                    lotes = get_lotes(venta)
                    dv["costo_unitario"], dv["precio_venta"] = 0, 0         
                    query = '''
                        SELECT lot.costo_unitario, prod.precio_venta 
                        FROM Prod pr join Lote lot on (pr.prod_id = lot.prod_id)
                        Where pr.prod_id = ?
                    '''
                    cur.execute(query,(dv["prod_id"]))
                    dv["costo_unitario"], dv["precio_venta"] = cur.fetchone()

                    ingreso_total += precio_venta * dv["cant"]
                    costo_total += costo_unitario * dv["cant"]

                query = '''
                    INSERT INTO Venta(ingreso_total,costo_total,ganancia,pago)
                    VALUES (?,?,?,?)
                    RETURNING venta_id
                '''
                ganancia = ingreso_total - costo_total 
                cur.execute(query,(ingreso_total,costo_total,ganancia,pago))
                venta_id = cur.fetchone()
                for dv in venta: 
                    dv["venta_id"] = venta_id
                query = ''' 
                    INSERT INTO Detalle_venta(prod_id,cantidad,costo_unitario,precio_unitario)
                    Values (:prod_id,:cant,:costo_unitario,:precio_unitario)
                '''
                cur.executemany(query,venta)


        try: 
            if self.conn: 
                cur = self.conn.cursor()
                ingreso_total,costo_total  =  0, 0 
                query = '''
                    INSERT INTO Venta(ingreso_total,costo_total,ganancia,pago)
                    VALUES (?,?,?,?)
                    RETURNING venta_id
                '''
                cur.execute(query,(0,0,0,pago))
                venta_id = cur.fetchone()

                for dv in venta:
                    query = '''
                        INSERT INTO Detalle_venta (prod_id,cantidad,costo_unitario,precio_unitario,venta_id)
                        SELECT p.prod_id, :cant, L.costo_unitario, p.precio_venta, :venta_id
                        FROM Prod p join Lote l on (p.prod_id = l.prod_id)
                        WHERE p.prod_id = :prod_id 
                        ORDER BY L.fecha_hora DESC
                        LIMIT 1 
                    '''
                    cur.execute(query,(dv["prod_id"]))
                    dv["costo_unitario"], dv["precio_venta"] = cur.fetchone()

                    ingreso_total += precio_venta * dv["cant"]
                    costo_total += costo_unitario * dv["cant"]

                query = '''
                    INSERT INTO Venta(ingreso_total,costo_total,ganancia,pago)
                    VALUES (?,?,?,?)
                    RETURNING venta_id
                '''
                ganancia = ingreso_total - costo_total 
                cur.execute(query,(ingreso_total,costo_total,ganancia,pago))
                venta_id = cur.fetchone()
x                    dv["venta_id"] = venta_id
                query = ''' 
                    INSERT INTO Detalle_venta(prod_id,cantidad,costo_unitario,precio_unitario)
                    Values (:prod_id,:cant,:costo_unitario,:precio_unitario)
                '''
                cur.executemany(query,venta)






