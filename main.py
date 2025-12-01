from database.database import GestorPrincipal 

def main():
	db = GestorPrincipal("data.db")

	db.crear_tablas()
	print("iniciado correctamente")

if __name__ == "__main__": 
	main()

db.set_