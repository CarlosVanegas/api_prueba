SELECT
    DATE_FORMAT(so.OrderDate, '%Y-%m') AS Mes,
    st.Name AS NombreTerritorio,
    SUM(CASE WHEN so.Status = 1 THEN 1 ELSE 0 END) AS TrProceso,
    SUM(CASE WHEN so.Status = 2 THEN 1 ELSE 0 END) AS TrAprobadas,
    SUM(CASE WHEN so.Status = 3 THEN 1 ELSE 0 END) AS TrAtrasadas,
    SUM(CASE WHEN so.Status = 4 THEN 1 ELSE 0 END) AS TrRechazadas,
    SUM(CASE WHEN so.Status = 5 THEN 1 ELSE 0 END) AS TrEnviadas,
    SUM(CASE WHEN so.Status = 6 THEN 1 ELSE 0 END) AS TrCanceladas,
    SUM(CASE WHEN so.Status = 1 THEN so.TotalDue ELSE 0 END) AS MntProceso,
    SUM(CASE WHEN so.Status = 2 THEN so.TotalDue ELSE 0 END) AS MntAprobadas,
    SUM(CASE WHEN so.Status = 3 THEN so.TotalDue ELSE 0 END) AS MntAtrasadas,
    SUM(CASE WHEN so.Status = 4 THEN so.TotalDue ELSE 0 END) AS MntRechazadas,
    SUM(CASE WHEN so.Status = 5 THEN so.TotalDue ELSE 0 END) AS MntEnviadas,
    SUM(CASE WHEN so.Status = 6 THEN so.TotalDue ELSE 0 END) AS MntCanceladas
FROM
    database_clients_bam.salesorderheader so
INNER JOIN
    database_clients_bam.salesterritory st ON so.TerritoryID = st.TerritoryID
GROUP BY
    DATE_FORMAT(so.OrderDate, '%Y-%m'),
    st.Name
ORDER BY
    DATE_FORMAT(so.OrderDate, '%Y-%m') ASC,
    st.Name ASC;