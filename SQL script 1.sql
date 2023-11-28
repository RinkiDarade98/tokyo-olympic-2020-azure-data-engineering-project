select * from athletes;

--count the number of athletes from each country
select Country, count(*) as Total_athletes from athletes
group by Country
order by Total_athletes DESC;

--count the number of athletes from each discipline or sports 
select Discipline, count(*) as Total_athletes from athletes
group by Discipline
ORDER BY Total_athletes DESC;

--athletes who participated in multiple sports 
SELECT PersonName, COUNT(DISTINCT Discipline) as count_of_sports from athletes
group by PersonName
HAVING COUNT(DISTINCT Discipline) > 1 
ORDER BY count_of_sports ;

--count of teams participated in each discipline by countries
SELECT TeamName, Discipline, COUNT(*) as count_teams from teams
GROUP BY TeamName, Discipline
ORDER by count_teams DESC;

--count of teams participated from each country
SELECT TeamName, Count(*) as count_teams from teams
GROUP BY TeamName
ORDER BY count_teams DESC;

--Top discipline in which women participated 
SELECT Discipline, count(*) count from teams 
WHERE Event ='Women'
GROUP BY Discipline
ORDER BY count DESC;

--countries having highest number of coaches 
SELECT Country, COUNT(*) from coaches
WHERE Event!='NULL'
GROUP BY Country
order by COUNT(*) DESC;

--distribution of coaches across the different countries 
SELECT Country, Discipline, Count(*) from coaches
GROUP BY Country, Discipline
ORDER BY Count(*) DESC;



--calculate the total medals won by each country
SELECT Team_Country, SUM(Gold) Total_Gold,
 SUM(Silver) Total_Silver, 
 SUM(Bronze) Total_Bronze FROM medals 
 GROUP BY Team_Country
 ORDER by Total_Gold DESC;


 --calculate the average number of entries by gender for each discipline 
 SELECT Discipline, 
 AVG(Female) avg_female,
 AVG(Male) avg_male
 FROM entriesgender
 GROUP BY Discipline;







