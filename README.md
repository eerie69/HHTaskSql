### Тестовое задание от Mindbox
### Техническое задание
В датафреймах (pyspark.sql.DataFrame) заданы продукты, категории и связь между ними. Одному продукту может соответствовать много категорий, в одной категории может быть много продуктов. Напишите метод с помощью PySpark, который вернет все продукты с их категориями (датафрейм с набором всех пар «Имя продукта – Имя категории»). В результирующем датафрейме должны также присутствовать продукты, у которых нет категорий.
### Решение 

Создаются DataFrames для Products, Categories со связью многие-ко-многим. Так же создается промежуточная таблица Product_categories,что бы хранить связи меджу Products и Categories.
```
Products
---------+-----------+
|productId|productName|
+---------+-----------+
|        1|  Product A|
|        2|  Product B|
|        3|  Product C|
|        4|  Product D|
+---------+-----------+
```
```
Categories
+----------+------------+
|categoryId|categoryName|
+----------+------------+
|         1|  Category X|
|         2|  Category Y|
|         3|  Category Z|
+----------+------------+
```
```
Product_categories
+----------+---------+
|categoryId|productId|
+----------+---------+
|         1|        1|
|         2|        1|
|         2|        2|
|         3|        3|
+----------+---------+
```

Выполняется SQL запрос на выборку всех пар ProductName - CategoryName, включая ProductName без категории.

```
SELECT p.productName, c.categoryName
    FROM products p
    LEFT JOIN product_categories pc ON p.productId = pc.productId
    LEFT JOIN categories c ON pc.categoryId = c.categoryId
```
Вывод
```
+-----------+------------+
|productName|categoryName|
+-----------+------------+
|  Product D|        null|
|  Product A|  Category X|
|  Product C|  Category Z|
|  Product A|  Category Y|
|  Product B|  Category Y|
+-----------+------------+
```
