 WITH TopProducts AS (
    SELECT
        DATE_FORMAT(so.OrderDate, '%Y-%m') AS Mes,
        p.Name AS Producto,
        SUM(sd.OrderQty) AS UnidadesDespachadas,
        ROW_NUMBER() OVER (PARTITION BY DATE_FORMAT(so.OrderDate, '%Y-%m') ORDER BY SUM(sd.OrderQty) DESC) AS Ranking
    FROM
        database_clients_bam.salesorderheader so
    INNER JOIN
        database_clients_bam.salesorderdetail sd ON so.SalesOrderID = sd.SalesOrderID
    INNER JOIN
        database_clients_bam.product p ON sd.ProductID = p.ProductID
    WHERE
        so.Status = 5 -- Considera solo órdenes despachadas
    GROUP BY
        DATE_FORMAT(so.OrderDate, '%Y-%m'),
        p.Name
),
TopCustomers AS (
    SELECT
        DATE_FORMAT(so.OrderDate, '%Y-%m') AS Mes,
        CONCAT(p.FirstName, ' ', p.LastName) AS Cliente,
        SUM(so.TotalDue) AS MontoTotalFacturado,
        ROW_NUMBER() OVER (PARTITION BY DATE_FORMAT(so.OrderDate, '%Y-%m') ORDER BY SUM(so.TotalDue) DESC) AS Ranking
    FROM
        database_clients_bam.salesorderheader so
    INNER JOIN
        database_clients_bam.customer c ON so.CustomerID = c.CustomerID
    INNER JOIN
        database_clients_bam.person p ON c.PersonID = p.BusinessEntityID
    WHERE
        so.Status = 5 -- Considera solo órdenes despachadas
    GROUP BY
        DATE_FORMAT(so.OrderDate, '%Y-%m'),
        p.FirstName,
        p.LastName
)
SELECT
    tp.Mes,
    MAX(CASE WHEN tp.Ranking = 1 THEN tp.Producto END) AS Producto_N1,
    MAX(CASE WHEN tp.Ranking = 2 THEN tp.Producto END) AS Producto_N2,
    MAX(CASE WHEN tp.Ranking = 3 THEN tp.Producto END) AS Producto_N3,
    MAX(CASE WHEN tc.Ranking = 1 THEN tc.Cliente END) AS Cliente_N1,
    MAX(CASE WHEN tc.Ranking = 2 THEN tc.Cliente END) AS Cliente_N2,
    MAX(CASE WHEN tc.Ranking = 3 THEN tc.Cliente END) AS Cliente_N3
FROM
    TopProducts tp
LEFT JOIN
    TopCustomers tc ON tp.Mes = tc.Mes
GROUP BY
    tp.Mes;
