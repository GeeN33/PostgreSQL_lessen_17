import psycopg2
import config

def db_connect():
    try:
        connection = psycopg2.connect(host=config.host, user=config.user, password=config.password, database=config.db_name)
        connection.autocommit = True
        return connection
    except Exception as ex:
        print(ex)
        return False

def request(connection, command):
    try:
        with connection.cursor() as cursor:
            cursor.execute(command)
            return cursor.fetchall()
    except Exception as ex:
        return ex
    finally:
        if connection:
            connection.close()

def print_rez(rez):
    if type(rez) == list:
        for r in rez:
            print(r)
    else:
        print(rez)

def main():
    print("""
    Введите номер команды если хотите 
    1.Вывести все объявления
    2.Вывести все объявления конкретных пользователей, пользователей пишите через запетую пример: пользователь1,пользователь2
    3.Вывести все объявления диапазоне цен, диапазон пишите через дефис пример: 10-30 
    4.Вывести все объявления для конкретного города
    5.Запрос, выполнена группировка пользователей с подсчетом количества общей цены пользователя
    """)
    number = input('>>')
    while not number.isnumeric():
        print('Это не номер команды попробуй ещё')
        number = input('>>')
    while not (0 < int(number) and int(number) < 6):
        print('Нет такой команды попробуй ещё')
        number = input('>>')

    connection = db_connect()
    if number == '1':
        command = """
                  SELECT * FROM ads                    
                 """
        rez = request(connection, command)
        print_rez(rez)
    if number == '2':
        user = input('>>')
        #user = 'Павел,Петр'
        users = tuple(user.split(','))
        command = f"""
                    SELECT ads.id, ads.name, ads_author.author, price, description, ads_address.address, is_published
                    FROM ads
                    JOIN ads_address ON ads_address.id = ads.fk_address
                    JOIN ads_author ON ads_author.id = ads.fk_author
                    WHERE ads_author.author IN {users}                 
                  """
        rez = request(connection, command)
        print_rez(rez)
    if number == '3':
        rang_str = input('>>')
        rang_str = '1000-2500'
        rang_ = rang_str.split('-')
        command = f"""
                SELECT ads.id, ads.name, ads_author.author, price, description, ads_address.address, is_published
                FROM ads
                JOIN ads_address ON ads_address.id = ads.fk_address
                JOIN ads_author ON ads_author.id = ads.fk_author
                WHERE price BETWEEN {rang_[0]} AND {rang_[1]} 
                ORDER BY price ASC              
                  """
        rez = request(connection, command)
        print_rez(rez)
    if number == '4':
        city_str = input('>>')
        command = f"""
                SELECT ads.id, ads.name, ads_author.author, price, description, ads_address.address, is_published
                FROM ads
                JOIN ads_address ON ads_address.id = ads.fk_address
                JOIN ads_author ON ads_author.id = ads.fk_author
                WHERE ads_address.address LIKE '{city_str}%'              
                  """
        rez = request(connection, command)
        print_rez(rez)
    if number == '5':
        command = f"""
                SELECT ads_author.author, SUM(price) 
                FROM ads
                JOIN ads_author ON ads_author.id = ads.fk_author
                GROUP BY ads_author.author
                ORDER BY SUM(price) DESC            
                  """
        rez = request(connection, command)
        print_rez(rez)

if __name__ == "__main__":
    main()