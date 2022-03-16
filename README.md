# Katerina-
import csv
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode


# create database
def create_database(cursor, DB_NAME):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed to create database {}".format(err))
        exit(1)


# create tables

def create_table_country(cursor):
    create_country_sql = "CREATE TABLE `country` (" \
                         "  `name` varchar (70) NOT NULL," \
                         " id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY" \
                         ") ENGINE=InnoDB"
    try:
        print("Creating table country: ")
        cursor.execute(create_country_sql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


def create_table_rate_by_country(cursor):
    create_country_sql = "CREATE TABLE `rates` (" \
                         "  `country_id` int NOT NULL," \
                         "  `rate_both` float(4,1) NOT NULL," \
                         "  `male` float(4,1) NOT NULL," \
                         "  `female` float(4,1) NOT NULL," \
                         " id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY" \
                         ") ENGINE=InnoDB"
    try:
        print("Creating table smoking rates by country: ")
        cursor.execute(create_country_sql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


def create_table_rate_death(cursor):
    create_country_sql = "CREATE TABLE `death` (" \
                         "  `country_id` int NOT NULL," \
                         "  `year` int NOT NULL," \
                         "  `rate` float (12,9) NOT NULL," \
                         " id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY" \
                         ") ENGINE=InnoDB"
    try:
        print("Creating table death rate: ")
        cursor.execute(create_country_sql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


def create_table_rate_sales(cursor):
    create_country_sql = "CREATE TABLE `sales` (" \
                         "  `country_id` int NOT NULL," \
                         "  `year` int NOT NULL," \
                         "  `sales_per_day` float (3,1) NOT NULL," \
                         " id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY" \
                         ") ENGINE=InnoDB"
    try:
        print("Sales of cigarettes per adult per day: ")
        cursor.execute(create_country_sql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


def create_table_rate_control(cursor):
    create_country_sql = "CREATE TABLE `control` (" \
                         "  `country_id` int NOT NULL," \
                         "  `year` int NOT NULL," \
                         "  `control` TINYINT," \
                         "  `budget` bigint," \
                         " id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY" \
                         ") ENGINE=InnoDB"
    try:
        print("Creating table control: ")
        cursor.execute(create_country_sql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


def create_country(name, cnx, cursor):
    sql = "SELECT id FROM country WHERE name = %s"
    p = cursor.execute(sql, (name, ))
    p = cursor.fetchall()
    if len(p) == 0:
        sql = "INSERT INTO country (name) VALUES (%s)"
        cursor.execute(sql, (name, ))
        cnx.commit()
        return cursor.lastrowid
    else:
        return p[0][0]


def import_rate_by_country(cnx, cursor):
    FILENAME = "rates_by_country.csv"

    sql = "INSERT INTO rates (country_id, rate_both, male, female) VALUES (%s, %s, %s, %s)"
    with open(FILENAME, "r", newline="") as file:
        reader = csv.reader(file)
        next(reader, None)
        for row in reader:
            id_country = create_country(row[0], cnx, cursor)
            row[0] = id_country
            cursor.execute(sql, row)

    cnx.commit()


def import_rate_death(cnx, cursor):
    FILENAME = "death_rate.csv"

    sql = "INSERT INTO death (country_id, year, rate) VALUES (%s, %s, %s)"
    with open(FILENAME, "r", newline="") as file:
        reader = csv.reader(file)
        next(reader, None)
        for row in reader:
            id_country = create_country(row[0], cnx, cursor)
            row[0] = id_country
            try:
                if row[2] !="" and row[2]!="None":
                    cursor.execute(sql, row)
            except BaseException as e:
                print(row)

    cnx.commit()


def import_rate_control(cnx, cursor):
    FILENAME = "control.csv"

    sql = "INSERT INTO control (country_id, year, control, budget) VALUES (%s, %s, %s,%s)"
    with open(FILENAME, "r", newline="") as file:
        reader = csv.reader(file)
        next(reader, None)
        for row in reader:
            id_country = create_country(row[0], cnx, cursor)
            row[0] = id_country
            for i in range(len(row)):
                if row[i] == "Data not available" or row[i] == "Not applicable" or row[i] == "":
                    row[i] = None
            if row[2]=="Yes":
                row[2] =1
            else:
                row[2] =0

            cursor.execute(sql, row)

    cnx.commit()

def import_rate_sales(cnx, cursor):
    FILENAME = "sales.csv"

    sql = "INSERT INTO sales (country_id, year, sales_per_day) VALUES (%s, %s, %s)"
    with open(FILENAME, "r", newline="") as file:
        reader = csv.reader(file)
        next(reader, None)
        for row in reader:
            id_country = create_country(row[0], cnx, cursor)
            row[0] = id_country
            cursor.execute(sql, row)

    cnx.commit()

# query 1 Average sales of a country
def avg_sales(cursor, name):
    sql = "select country.name, AVG(sales_per_day) from sales inner join country on sales.country_id=country.id where country.name=%s group by country.name"
    cursor.execute(sql, (name,))
    results = cursor.fetchall()
    for k in results:
        if k[0] is not None and k[1] is not None:
            print(k[0], k[1].real)

#  query 2 Search countries details which related to government  control
def control_country(cursor,budget):
    sql = "select country.name, control.year, control.budget from control inner join  country on control.country_id=country.id where control='1' and budget>%s"
    cursor.execute(sql,(budget,))
    result = cursor.fetchone()
    if result is None:
        print('country not found')
    else:
        print(result)


# query 3
def list_of_countries(cursor):
    sql = "Select distinct country.name, rate_both, male, female, control.control,death.rate,death.year " \
          "From Rates Inner join Control on control.country_id=rates.country_id Inner join death " \
          "on control.country_id=death.country_id Inner join Country on control.country_id=country.id " \
          "Order by death.rate DESC"
    cursor.execute(sql)
    results = cursor.fetchall()
    for k in results:
        print(k[0],k[1],k[2],k[3],k[4],k[5],k[6])

# query 4
def choose_of_country(cursor,name):
    sql="Select distinct country.name, sales_per_day, control.control, sales.year, death.rate" \
        " From Sales, Control, Death, Country Where sales.country_id=control.country_id" \
        " and control.country_id=death.country_id and death.country_id=country.id and country.name = %s"
    cursor.execute(sql, (name,))
    result = cursor.fetchall()
    for k in result:
        print(k[0], k[1], k[2], k[3], k[4])


# query 5
def maximum_consumption(cursor):
    sql = "Select country.name, MAX(rate_both), MAX(death.rate) from rates " \
          "Inner Join country on rates.country_id=country.id " \
          "Inner join death on country.id=death.country_id group by country.name"
    cursor.execute(sql)
    results = cursor.fetchall()
    for k in results:
        print(k[0], k[1], k[2])


#create view

def create_view(cursor):
    sql="CREATE VIEW Cigarettes_country_8 as select country.name, rates.rate_both, sales. sales_per_day, " \
        "death.rate, control.control" \
        " From Country, Rates, Sales, Control, Death Where control.control='1'"
    #cursor.execute(sql)
    #sql=" select * from Cigarettes_country_4"
    #cursor.execute(sql)
    #results = cursor.fetchall()
    #for k in results:
    #    print(k[0], k[1], k[2], k[3], k[4])
    try:
        print("Creating view: ")
        cursor.execute(sql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

def main():
    #cnx = mysql.connector.connect(user='root', password='', host='localhost')
    cnx = mysql.connector.connect(user='Katya', password='15071983', host='localhost')
    DB_NAME = 'cigarettes905'
    cursor = cnx.cursor()

    try:
        cursor.execute("USE {}".format(DB_NAME))
        cnx.database = DB_NAME
    except mysql.connector.Error as err:
        print("Database {} does not exist".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor, DB_NAME)
            print("Database {} created succesfully.".format(DB_NAME))
            cnx.database = DB_NAME
            create_table_country(cursor)
            create_table_rate_by_country(cursor)
            create_table_rate_death(cursor)
            create_table_rate_sales(cursor)
            create_table_rate_control(cursor)

            import_rate_by_country(cnx, cursor)
            import_rate_death(cnx, cursor)
            import_rate_control(cnx, cursor)
            import_rate_sales(cnx, cursor)

        else:
            print(err)

    while True:
        print("1.Average sales of a country")
        print("2.Search countries details which related to government control")
        print("3.List of countries where the following information are presented such as name of country, consumption cigarettes, existing control from a government and death rate.")
        print("4.A user can choose a country to see the following information: sales per day, existing control, year, death rate.")
        print("5.List countries where maximum consumption of cigarettes and maximum death rates.")
        print("6.Create view ")

        action = int(input())


        if action == 1:
            name = input('Enter name of a country: ')
            avg_sales(cursor, name)
        elif action == 2:
            budjet = int(input('Enter budget of a country '))
            control_country(cursor, budjet)
        elif action == 3:
            list_of_countries(cursor)
        elif action == 4:
            name = input('Enter name of a country: ')
            choose_of_country(cursor,name)
        elif action == 5:
            maximum_consumption(cursor)
        elif action == 6:
            create_view(cursor)


main()
