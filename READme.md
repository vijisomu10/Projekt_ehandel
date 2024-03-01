Projekt – e-handelsapplikation

1 Simulera e-handelsapplikation! 
1.1 Start 
Det här projektet besår av att ni ska bygga en simulerande e-handelsapplikation där vikten kommer ligga på hantering av data. 
1.2 Delar 
Applikationen ska dels bestå av ett program som simulerar försäljning av ehandelsvaror på nätet genom att strömma färdiga ordrar till Apache Kafka. Sedan ska det finnas program som konsumerar denna data för olika ändamål. 
1.2.1 1:a konsumenten 
Vi har en konsument som vill veta hur många ordrar vi har fått in från kl 00.00 till nuvarande tidpunkt. (Brytpunkt varje dag kl 00.00 alltså) 
1.2.2 2:a konsumenten 
Vi vill veta lite om försäljning!! Vi vill ha dagens totala försäljning hittils och den senaste timmens försäljning! 
1.2.3 3:e konsumenten 
Varje 24:e timme, aka vid 00:00 ska en daglig rapport skapas! Antal ordrar, summa på försäljning, antal för vardera produkt såld under den dagen. (sparas till en fil med dagens datum)! 
1.2.4 4:e konsumenten 
För varje försäljning så ska saldo för produkten uppdateras! Vi vill alltså ha ett lagersaldo tillgängligt att kunna ta ifrån när en försäljning av en produkt sker. (Tänk på detta vid simulering av försäljningen ovan!) 
1.2.5 VG
Det ska finnas ett orderhanteringssystem...
Varje order ska ha en procentuell chans att hanteras efter försäljning. Vid hantering ska ordern ”hanteras” och ett mail om att ordern är skickad ska utfärdas till kunden (info skrivs till en fil)! Därefter ska orden markeras som hanterad, detta innebär att det finns en kontroll/övervakning på vilka ordrar som är hanterade! Dessa ska ligga i en egen kafka-topic. Det ska också finnas en övervakning på hur många ordrar som är hanterad de senaste 5 minuterna, senaste halvtimmen, senaste timmen, senaste två timmarna och medelvärdet för var 5 minuts hantering de senaste två timmarna!

Lösning:
1. Producer - produced Orders i Kafka Producer med products.txt
2. Order_created_now - for total Orderdetaljer, produced_orders - bara sista order entry
3. Consumer1.py - hur många ordrar fått in från kl 00.00 till nuvarande tidpunkt. 
4. Consumer2.py - Total sales och sista timmens sales
5. Consumer3.py - Daglig rapport med antal ordrar, summa på försäljning, antal för vardera produkt såld under den dagen. Sales_update_Consumer3 mappan har daglig rapport
6. Consumer4.py - Product saldo uppdateras i sqlite databasen
7. Tabular data har allt txt file
8. Koden fungerar med disttributes event streaming platform 
