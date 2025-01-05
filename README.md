# **ETL proces datasetu Northwind**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z datasetu **Northwind**. Projekt sa zameriava na spracovanie obchodných údajov vrátane predajov, zákazníkov, zamestnancov a produktov. Výsledný dátový model je optimalizovaný na multidimenzionálnu analýzu a poskytuje základ pre vizualizáciu kľúčových metrík.

---

## **1. Úvod a popis zdrojových dát**

Cieľom projektu je analyzovať obchodné dáta zo systému Northwind. Táto analýza identifikuje trendy v predajoch, najčastejšie objednávané produkty, výkonnosť zamestnancov a významných zákazníkov.

Zdrojové dáta pochádzajú z demo datasetu Northwind, ktorý obsahuje údaje o zákazníkoch, zamestnancoch, produktoch, objednávkach a ďalších aspektoch obchodnej činnosti. Hlavné tabuľky sú:

- **`categories_staging`**: Kategórie produktov s popisom a identifikátorom.
- **`customers_staging`**: Zákazníci s kontaktnými údajmi, adresou a krajinou.
- **`employees_staging`**: Zamestnanci vrátane osobných údajov a poznámok.
- **`shippers_staging`**: Informácie o prepravných spoločnostiach a ich kontaktoch.
- **`suppliers_staging`**: Dodávatelia produktov s adresou a kontaktnými údajmi.
- **`products_staging`**: Podrobné informácie o produktoch, vrátane cien a dodávateľov.
- **`orders_staging`**: Objednávky s odkazmi na zákazníkov a zamestnancov.
- **`orderdetails_staging`**: Detaily objednávok, ako sú produkty a ich množstvo.

ETL proces sa zameriava na extrakciu, transformáciu a načítanie týchto dát pre účely analýzy.

---

### **1.1 Dátová architektúra**

### **ERD diagram**

Surové dáta sú organizované v relačnom modeli, ktorý je znázornený v **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/Lucasinoo27/DATABASE-NORTHWIND/blob/main/Northwind_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1: ERD schéma Northwind</em>
</p>

---

## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, ktorý umožňuje efektívnu analýzu obchodných dát. Centrálnym bodom je faktová tabuľka **`fact_orders`**, prepojená s nasledujúcimi dimenziami:

- **`dim_products`**: Informácie o produktoch, ako názvy, kategórie a ceny.
- **`dim_customers`**: Demografické údaje o zákazníkoch, ako adresy a krajiny.
- **`dim_employees`**: Informácie o zamestnancoch zodpovedných za objednávky.
- **`dim_suppliers`**: Dodávatelia produktov s ich lokalitami.
- **`dim_shippers`**: Detaily prepravcov zapojených do doručenia objednávok.
- **`dim_date`**: Kalendárne údaje o objednávkach (deň, mesiac, rok).
- **`dim_time`**: Časové údaje (hodiny, AM/PM).

Model umožňuje jednoduché spojenie faktovej tabuľky s dimenziami, čím sa zlepšuje interpretácia a rýchlosť analýz.

<p align="center">
  <img src="https://github.com/Lucasinoo27/DATABASE-NORTHWIND/blob/main/STAR_SCHEME_PICTURE.png" alt="Star Schema">
  <br>
  <em>Obrázok 2: Hviezdicový model Northwind</em>
</p>

---

## **3. ETL proces v Snowflake**

ETL proces bol rozdelený do troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces umožnil spracovanie pôvodných dát zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

### **3.1 Extract (Extrahovanie dát)**

Zdrojové dáta vo formáte `.csv` boli nahraté do Snowflake pomocou interného stage úložiska s názvom `OTTER_NORTHWIND_stage`. Stage funguje ako dočasné úložisko na import alebo export dát. Na vytvorenie stage bol použitý príkaz:

```sql
CREATE OR REPLACE STAGE OTTER_NORTHWIND_stage;
```

Dáta boli následne nahraté do staging tabuliek pomocou príkazu `COPY INTO`. Príklad príkazu pre import údajov do tabuľky `categories_staging`:

```sql
COPY INTO categories_staging
FROM @OTTER_NORTHWIND_stage/northwind_table_categories.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

Tento postup bol zopakovaný pre všetky zdrojové tabuľky, čím sa zabezpečilo ich pripravenie na ďalšie spracovanie.

### **3.2 Transform (Transformácia dát)**

Transformácia dát zahŕňala čistenie, obohacovanie a prípravu dimenzií a faktovej tabuľky. Nasledujúce kroky ukazujú, ako boli vytvorené jednotlivé dimenzie a faktová tabuľka:

#### Dimenzia `DIM_DATE`
Táto dimenzia uchováva informácie o dátumoch objednávok. Obsahuje odvodené údaje ako deň, mesiac, rok a typ dňa (pracovný deň alebo víkend):

```sql
CREATE OR REPLACE TABLE DIM_DATE AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('DAY', OrderDate)) AS dim_dateID,
    DATE_TRUNC('DAY', OrderDate) AS date,
    EXTRACT(YEAR FROM OrderDate) AS year,
    EXTRACT(MONTH FROM OrderDate) AS month,
    EXTRACT(DAY FROM OrderDate) AS day,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM OrderDate) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM orders_staging;
```

#### Faktová tabuľka `FACT_ORDERS`
Táto tabuľka obsahuje kľúčové metriky a prepojenia na dimenzie:

```sql
CREATE OR REPLACE TABLE FACT_ORDERS AS
SELECT
    o.OrderID AS fact_orderID,
    o.OrderDate AS order_date,
    d.dim_dateID AS date_id,
    c.dim_customerID AS customer_id,
    e.dim_employeeID AS employee_id,
    s.dim_supplierID AS supplier_id,
    sh.dim_shipperID AS shipper_id,
    p.dim_productID AS product_id,
    p.category_id AS dim_categoryID,
    od.Quantity AS quantity,
    p.price AS unit_price,
    od.Quantity * p.price AS total_price
FROM orders_staging o
JOIN orderdetails_staging od ON o.OrderID = od.OrderID
JOIN DIM_DATE d ON DATE_TRUNC('DAY', o.OrderDate) = d.date
JOIN DIM_CUSTOMERS c ON o.CustomerID = c.dim_customerID
JOIN DIM_PRODUCTS p ON od.ProductID = p.dim_productID
JOIN DIM_SUPPLIERS s ON p.category_id = s.dim_supplierID
JOIN DIM_EMPLOYEES e ON o.EmployeeID = e.dim_employeeID
JOIN DIM_SHIPPERS sh ON o.ShipperID = sh.dim_shipperID;
```

### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli staging tabuľky odstránené na optimalizáciu úložiska:

```sql
DROP TABLE IF EXISTS categories_staging;
DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS employees_staging;
DROP TABLE IF EXISTS shippers_staging;
DROP TABLE IF EXISTS suppliers_staging;
DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS orderdetails_staging;
```

ETL proces v Snowflake pripravil dáta z pôvodného formátu na analýzu, čím sa umožnila efektívna vizualizácia obchodných výsledkov a trendov.

---

## **4 Vizualizácia dát**

Dashboard obsahuje `5 vizualizácií`, ktoré poskytujú prehľad o obchodných výsledkoch, trendoch a kľúčových metrikách spojených s objednávkami, produktmi a zákazníkmi v Northwind dataset.

<p align="center">
  <img src="https://github.com/Lucasinoo27/DATABASE-NORTHWIND/blob/d20b4fde242969e26218125bf2d40277f8bf93e2/DASHBOARD_PICTURE_VISUALIZATION.png" alt="Northwind Dashboard">
  <br>
  <em>Obrázok 3: Dashboard Northwind dataset</em>
</p>

---

### **Graf 1: Mesačný počet objednávok**
Táto vizualizácia ukazuje počet objednávok rozdelených podľa jednotlivých mesiacov v každom roku. Pomáha identifikovať sezónne trendy a obdobia s najvyššou aktivitou objednávok.

```sql
SELECT
    d.year,
    d.month,
    COUNT(f.fact_orderID) AS order_count
FROM FACT_ORDERS f
JOIN DIM_DATE d ON f.date_id = d.dim_dateID
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

---

### **Graf 2: Najpredávanejšie produkty**
Graf zobrazuje top 5 produktov s najvyšším celkovým predaným množstvom. Umožňuje identifikovať najobľúbenejšie produkty a optimalizovať skladové zásoby.

```sql
SELECT
    p.product_name,
    SUM(f.quantity) AS total_quantity
FROM FACT_ORDERS f
JOIN DIM_PRODUCTS p ON f.product_id = p.dim_productID
GROUP BY p.product_name
ORDER BY total_quantity DESC
LIMIT 5;
```

---

### **Graf 3: Objednávky podľa krajiny**
Táto vizualizácia znázorňuje počet objednávok pre jednotlivé krajiny. Pomáha analyzovať geografické rozloženie zákazníkov a odhaliť trhy s najväčším dopytom.

```sql
SELECT
    c.country,
    COUNT(f.fact_orderID) AS order_count
FROM FACT_ORDERS f
JOIN DIM_CUSTOMERS c ON f.customer_id = c.CustomerID
GROUP BY c.country
ORDER BY order_count DESC;
```

---

### **Graf 4: Ročný výnos z objednávok**
Vizualizácia ukazuje celkový výnos z objednávok rozdelený podľa rokov. Umožňuje identifikovať trendy v príjmoch a hodnotiť výkon podniku počas viacerých rokov.

```sql
SELECT
    d.year,
    SUM(f.total_price) AS total_revenue
FROM FACT_ORDERS f
JOIN DIM_DATE d ON f.date_id = d.dim_dateID
GROUP BY d.year
ORDER BY d.year;
```

---

### **Graf 5: Príjmy podľa dodávateľov**
Graf znázorňuje príjmy generované jednotlivými dodávateľmi. Pomáha identifikovať kľúčových partnerov, ktorí prispievajú najviac k celkovým príjmom.

```sql
SELECT
    s.supplier_name,
    SUM(f.total_price) AS supplier_revenue
FROM FACT_ORDERS f
JOIN DIM_SUPPLIERS s ON f.supplier_id = s.dim_supplierID
GROUP BY s.supplier_name
ORDER BY supplier_revenue DESC;
```

---

Dashboard poskytuje jasný a zrozumiteľný pohľad na obchodné dáta, čo umožňuje lepšie rozhodovanie, plánovanie a optimalizáciu procesov.

**Autor:** Lukas Sutka