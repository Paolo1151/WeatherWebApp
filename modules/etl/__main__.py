import manager

def main():
    mngr = manager.WeatherDatabaseManager()
    mngr.perform_etl()


if __name__ == '__main__':
    main()

