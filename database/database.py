import sqlite3

class GestorPrincipal: 

    def __init__(self,db_file):
        try:
            self.conn = sqlite3.connect(db_file)
            print("Conexion Exitosa")
        except Exception as e: 
            print("Error en la conexion por:",e)
            self.conn = None
        
    def crear_tablas(self): 
        try:
            if self.conn:
                cur = self.conn.cursor() 
                query = ''' 
                CREATE TABLE Socios ( 
                    soc_id INT PRIMARY KEY AUTOINCREMENT, 
                    activo BOOLEAN ,
                    fecha_inicio DATE, 
                    DNI INT NOT NULL UNIQUE, 
                    nombre TEXT, 
                    apellido TEXT, 
                    telefono TEXT
                );

                CREATE TABLE Planes ( 
                    plan_id INT PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT, 
                    precio REAL, 
                    duracion TEXT, 
                    descripcion TEXT, 
                );

                CREATE TABLE Pagos (
                    soc_id INT PRIMARY KEY,
                    pago_id INT PRIMARY KEY AUTOINCREMENT, 
                    monto_total REAL,
                    forma TEXT, 
                    FOREIGN KEY (soc_id) REFERENCES Socios(soc_id) 
                );

                CREATE TABLE Asistencias (
                    asis_id INT PRIMARY KEY AUTOINCREMENT,
                    fecha_hora DATETIME DEFAULT CURRENT_TIMESTAMP,  
                    soc_id INT ,
                    estado BOOLEAN,
                    tipo TEXT, 
                    FOREIGN KEY (soc_id) REFERENCES Socios(soc_id)
                )

                '''
                cur.executescript(query)
        except Exception as e: 
            print("Error:",e)


    def agregar_socio(self,socio): 
        try:
            if self.conn: 
                cur = self.conn.cursor()
                sql = ''' INSERT INTO Socios '''




        except Exception as e: 
            print("Error:",e)