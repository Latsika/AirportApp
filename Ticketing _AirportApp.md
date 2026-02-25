# Ticketing _AirportApp - User Manual

Tento manual popisuje komplet funkcionalitu AirportApp pre End Usera aj Admina.

## 1. Co je AirportApp

1. AirportApp je lokalna desktop aplikacia spustena cez `AirportApp.exe`.
2. Po spusteni sa app zobrazi v prehliadaci na lokalnej adrese `http://127.0.0.1:<port>`.
3. Nepotrebuje instalovany Python na cielovom PC.
4. Data su lokalne v SQLite databaze `airport_app.db` v tom istom priecinku ako `.exe`.

## 2. Co preniest na nove PC

1. Na bezne pouzitie prenasaj cely priecinok aplikacie, nie len `.exe`.
2. Minimalne subory/priecinky:
- `AirportApp.exe`
- `airport_app.db` (ak uz mas data)
- `backups\` (odporucane)
- `logs\` (odporucane)
3. Ak prenasas iba `AirportApp.exe` bez `airport_app.db`, vytvori sa nova prazdna databaza.

## 3. Spustenie aplikacie

1. Dvojklik na `AirportApp.exe`.
2. Otvori sa login stranka v prehliadaci.
3. Pri starte app automaticky:
- inicializuje DB schema (migracie bez straty dat),
- vytvori/udrzi `logs\app.log`,
- vykona automaticke kontroly notifikacii a report email scheduler.

## 4. Subory a priecinky, ktore app vytvara

1. `airport_app.db`:
- hlavna databaza (users, airlines, sales, report snapshots, rewards, nastavenia).
2. `backups\`:
- automaticke zalohy databazy pri starte app,
- zalohy pri update cez `install_update.exe`.
3. `logs\app.log`:
- technicke logy aplikacie.
4. `crash.log`:
- vznikne pri kritickej chybe startu.

## 5. Prihlasenie, registracia, reset hesla

1. Login polia:
- `Name or Nickname`
- `Password`
2. Pri novej prazdnej DB sa vytvori default admin:
- Nickname: `Admin`
- Password: `12345`
3. Pri prvom logine je mozne vynutene zmenit heslo.
4. `Sign Up`:
- registracia noveho usera,
- ucet caka na schvalenie (`Admin` alebo `Deputy`).
5. `Forgot Password`:
- obnova cez bezpecnostne otazky.
6. Auto logout:
- session timeout po 30 minutach neaktivity.

## 6. Role a opravnenia

1. `User`:
- predaj,
- reporty podla dostupnych menu.
2. `Deputy`:
- schvalovanie userov.
3. `Admin`:
- plna sprava users, airlines, fees, sales edit/delete, reports, notifications, variable rewards, account settings.

## 7. Predaj (Sales)

### 7.1 New Sale

1. Otvor `Sales -> New Sale`.
2. Vyber airline.
3. Vyber destination.
4. Zadaj volitelne:
- PNR
- Passenger name
5. Vyber poplatky:
- airline fees,
- airport service fees.
6. Vyber platbu:
- `CASH` alebo `CARD`.
7. Uloz predaj.
8. Kazda polozka sa uklada ako samostatny riadok v sale items.

### 7.2 Sales List

1. Otvor `Sales -> Sales List`.
2. Filtrovanie/hladanie podla PNR alebo mena.
3. Akcie:
- `Edit` predaja,
- `Delete` predaja (admin).
4. Zmeny sa ukladaju do sales logov.

## 8. Airlines, destinations, fees (Admin)

1. `Airlines`:
- pridanie airline,
- edit airline,
- delete airline.
2. `Airline Destinations`:
- pridanie destinacie (nazov + IATA kod, napr. `KSC`, `BTS`),
- edit/delete destinacie.
3. `Airline Fees`:
- pridanie fee pre konkretnu airline,
- edit/delete fee.
4. `Airport Service Fees`:
- centralne letiskove poplatky,
- edit/delete.

## 9. Co sa stane pri zmene ceny fee

1. Zmena ceny fee neprepise historicke predaje.
2. Historicke zaznamy ostanu s povodnou cenou.
3. Nove predaje po zmene pouziju novu cenu.

## 10. Reports

### 10.1 Typy reportov

1. `Daily Report`
2. `Monthly Report`
3. `Custom Report`

### 10.2 Exporty

1. Reporty sa daju exportovat do:
- PDF
- CSV (kde je podporovane).
2. Vytvorenie reportu sa uklada do `report_snapshots`.

### 10.3 Custom Report logika (dolezite)

1. Ak vyberies iba airline fees pre konkretnu airline:
- v reporte aj PDF sa zobrazia len tieto airline fees.
- nezobrazi sa zavadzajuca sekcia navyse.
2. Ak vyberies airline fees + airport fees:
- report zobrazi airline fees,
- plus suhrn `All Fees Total` (airline + airport).
3. V detail sekcii `Airline Detail Report` je aj destination code:
- UI, CSV aj PDF obsahuje kratky kod (napr. `KSC`, `BTS`).
4. Detailny zoznam obsahuje:
- Date (kazda predana polozka solo riadok),
- PNR,
- Passenger Name,
- Airline Fee,
- Destination (code),
- Amount,
- Cash/Card.
5. Na konci su totaly:
- total za kazdy fee typ zvlast,
- finalny celkovy total.

## 11. Variable Rewards

1. `Variable Rewards` pracuje s mesiacom/rokom.
2. Vypocet je naviazany na airport fees total za zvoleny mesiac.
3. Moznosti:
- aktivovat/deaktivovat usera pre rewards,
- nastavit globalne percento,
- manualne upravit odmenu pre konkretneho usera,
- `Save` snapshotu.
4. Exporty:
- PDF pre vsetkych,
- PDF pre jedneho usera (`Print PDF` pri mene),
- summary PDF za rozsah mesiacov.

## 12. Users a bezpecnost

1. `Users`:
- schvalit cakatelov,
- edit usera,
- reset hesla,
- reset security otazok,
- delete usera.
2. `User logs`:
- auth historia,
- sales aktivita.
3. `Reassign Admin`:
- presun admin prav na iny ucet.

## 13. Notifications a emaily

1. Nastavenie prijemcov:
- `Notifications` obrazovka, max 10 email adries.
2. Notification templates:
- vytvorenie/uprava sablon.
3. SMTP:
- `Account Settings -> SMTP`.
4. Odosielatel emailu:
- priorita:
  - `smtp_sender` z app settings,
  - inak `SMTP_SENDER` z prostredia,
  - inak SMTP user,
  - fallback `no-reply@airportapp.local`.

## 14. Automaticke denny/mesacny report email

1. Scheduler sa spusta automaticky pri aktivite app (before request).
2. Denny report:
- posiela sa po 00:05 lokalneho casu za predchadzajuci den.
3. Mesacny report:
- posiela sa po 00:05 prveho dna noveho mesiaca za predchadzajuci mesiac.
4. Catch-up mechanizmus:
- ak app nebezala a reporty sa neposlali, po dalsom spusteni/dopyte ich doposle.
5. Email obsahuje PDF prilohu reportu.
6. Duplicity sa blokuju cez snapshot kluce:
- `daily_auto_email`,
- `monthly_auto_email`.

## 15. Account Settings (Admin)

1. SMTP konfiguracia:
- host, port, user, password, sender, TLS.
2. DB export:
- stiahnutie aktualnej `airport_app.db`.

## 16. Update bez straty dat

### 16.1 Standardny postup cez updater

1. Pouzi `install_update.exe`.
2. Vyber priecinok, kde je existujuci `AirportApp.exe`.
3. Updater:
- zastavi beziaci proces app,
- spravi zalohu DB do `backups\airport_app_update_<timestamp>.db`,
- nahradi iba `AirportApp.exe`.
4. Data (users, airlines, sales, reports, rewards) ostanu zachovane.

### 16.2 Co posielat uzivatelovi

1. Na bezny update staci poslat `install_update.exe`.
2. Na novu instalaciu (first run) posli balik s `AirportApp.exe`.

## 17. Troubleshooting

### 17.1 App sa nespusti

1. Skontroluj, ci mas pravo zapisovat do priecinka aplikacie.
2. Skontroluj `crash.log` v priecinku app.
3. Skontroluj `logs\app.log`.
4. Ak je priecinok read-only (napr. USB s ochranou), spusti app z lokalneho disku.

### 17.2 Chyba `no such table: app_state`

1. Znamena to nekonzistentnu alebo staru DB schema.
2. Riesenie:
- spusti novu verziu `AirportApp.exe` v priecinku s platnou `airport_app.db`,
- ak chyba trva, obnov DB zo `backups\`.

### 17.3 Login hlasi `Invalid credentials`

1. Over presny `Name or Nickname`.
2. Skontroluj velkost pismen a rozlozenie klavesnice.
3. Pouzi `Forgot Password`.
4. Admin moze resetnut heslo v `Users`.

### 17.4 Neodosielaju sa email reporty/notifikacie

1. Over SMTP (`host`, `port`, `user`, `password`, TLS).
2. Over, ze su nastavene recipient emaily.
3. Over internet konektivitu a firewall pre SMTP port.
4. Pozri `logs\app.log` na SMTP chyby.

### 17.5 PDF/CSV sa nevygeneruje

1. Skontroluj, ci su data v zvolenom intervale.
2. Skontroluj prava zapisovat do cieloveho priecinka.
3. Vyskusaj iny priecinok (napr. Desktop).

### 17.6 Updater zlyha

1. Zavri beziaci AirportApp.
2. Spusti `install_update.exe` ako user s pravami zapisovat do cieloveho priecinka.
3. Over, ze vyberas priecinok, kde je `AirportApp.exe`.
4. Ak je `.exe` blokovane antivirusom, povol vynimku.

### 17.7 App z USB je pomala alebo nestabilna

1. Je to mozne pri pomalom USB.
2. Odporucanie:
- pouzivat z lokalneho SSD/HDD,
- USB pouzit primarne na prenos.

## 18. Prevadzkove odporucania

1. Pravidelne kopiruj `airport_app.db` a `backups\` mimo zariadenia.
2. Pred kazdym update nechaj app zatvorenu.
3. Pravidelne kontroluj `logs\app.log`.
4. Testuj report export (PDF/CSV) po vacsich zmenach konfiguracie.

## 19. Rychly checklist pre End Usera

1. Spustit `AirportApp.exe`.
2. Prihlasit sa.
3. Zadavat predaje (airline, destination, fee, payment).
4. Vygenerovat reporty podla potreby (daily/monthly/custom).
5. Pri update spustit `install_update.exe` a vybrat priecinok aplikacie.
6. Neriesit manualnu migraciu DB, robi sa automaticky.
- Delete sale (podla role)

## 6. Reporty

### 6.1 Daily Report

1. Otvor `Reports -> Daily`.
2. Vyber datum.
3. Moznosti exportu:
- CSV
- PDF

### 6.2 Monthly Report

1. Otvor `Reports -> Monthly`.
2. Vyber mesiac.
3. Moznosti exportu:
- CSV
- PDF

### 6.3 Custom Report

1. Otvor `Reports -> Custom`.
2. Nastav filtre:
- Date from / Date to
- Airline / Airport fees
- Destination
- Service/Fee
- Sold by
- Payment method

3. Klikni `Load report`.
4. Klikni `SAVE` pre PDF alebo `Export .csv`.

5. Dolezite spravanie totalov:
- Ak vyberies iba airline fees, report ukaze iba airline casti.
- `All Fees Total` sa zobrazi len pri kombinacii airline + airport fees.

6. V `Airline Detail Report` je stlpec `Destination` iba ako kod (napr. `KSC`, `BTS`), rovnako v CSV aj PDF.

## 7. Variable Rewards

1. Otvor `Variable rewards of users`.
2. Vyber mesiac a rok.
3. Aplikacia pocita rewards z airport fees.
4. Mozes:
- Nastavit percento
- Nastavit manualnu sumu na usera
- Aktivovat/deaktivovat usera pre rewards

5. Tlacidla:
- `Save`
- `Save PDF` (vsetci)
- `Print` / `Print PDF` pre konkretneho usera
- `Yearly Summary`

6. Yearly Summary:
- Filter od-do mesiaca
- Export celkoveho summary PDF
- Print PDF po jednotlivych useroch

## 8. Sprava pouzivatelov (Admin/Deputy)

1. `Users` zoznam:
- pending approval
- approved users

2. Akcie:
- Approve
- Edit user
- Delete user (Admin)
- Reset password (Admin)
- Reset security questions
- User logs

3. Reassign admin:
- Ak ostava posledny admin, system nedovoli jeho odstranenie bez reassignmentu.

## 9. Sprava airlines, fees a destinations (Admin)

1. Airlines:
- Add airline
- Edit airline
- Delete airline

2. Airline fees:
- Add/Edit/Delete fee
- Kluc fee (`fee_key`) ma byt unikatny v ramci airline

3. Airport service fees:
- Add/Edit/Delete globalnych airport fee poloziek

4. Destinations:
- Add/Edit/Delete destination pre konkretnu airline
- V reporte sa pouzivaju destination kody

## 10. Notifikacie a emaily

### 10.1 SMTP

1. Otvor `Account settings`.
2. Vypln:
- SMTP host
- SMTP port
- SMTP user
- SMTP password
- Sender (odporucane)
- TLS on/off

3. Uloz `Save SMTP`.

### 10.2 Notification recipients

1. Otvor `Create notifications`.
2. Nastav emaily prijemcov.
3. Nastav/aktivuj sablony notifikacii.

### 10.3 Automaticke report emaily

1. Daily PDF report:
- odoslanie po `00:05` nasledujuceho dna
- ak app nebezala, po spusteni dobehne chybajuce dni (catch-up)

2. Monthly PDF report:
- odoslanie po `00:05` po zmene mesiaca
- po spusteni app dobehne chybajuce mesiace (catch-up)

3. Priloha:
- automaticke daily/monthly reporty posielaju PDF v prilohe

4. Bezne textove notifikacie:
- created/not_created udalosti mozu byt bez prilohy (podla typu notifikacie)

## 11. Data, zalohy a prenos

1. Vsetky data su lokalne v `airport_app.db`.
2. Pri starte sa robi automaticka zalohova kopia DB do `backups`.
3. Retencia zaloh je obmedzena (stare sa mazu automaticky).
4. Pri prenose na iny PC:
- Ak chces prenies data, prenes `AirportApp.exe` aj `airport_app.db`.
- Ak prenesies iba EXE, vytvori sa nova prazdna DB.

## 12. Update aplikacie bez straty dat

1. Dostanes `install_update.exe`.
2. Spustis `install_update.exe`.
3. Vyberies priecinok, kde je aktualny `AirportApp.exe`.
4. Updater:
- zastavi beziaci proces
- spravi DB backup
- nahradi iba `AirportApp.exe`
- ponecha `airport_app.db`

5. Po update ostanu zachovane:
- users
- airlines
- fees
- predaje
- report data

## 13. Troubleshooting

### 13.1 Invalid credentials pri logine

1. Skontroluj velkost pismen v mene/nicku.
2. Pri novej DB skus `Admin / 12345`.
3. Ak stale nefunguje, skontroluj ci nepouzivas iny priecinok s inou DB.

### 13.2 Aplikacia sa nespusti

1. Pozri `crash.log` v priecinku aplikacie.
2. Skontroluj prava na zapis do priecinka.
3. Spusti z lokalneho disku (niekedy firemne politiky blokuju spustenie z USB).

### 13.3 Emaily sa neposielaju

1. Skontroluj SMTP host/port/user/password.
2. Skontroluj sender email.
3. Skontroluj zoznam recipients v notifications.
4. Over firewall/proxy.
5. Pozri `logs\\app.log`.

### 13.4 Report email neprisiel o 00:05

1. App musi bezat alebo sa musi spustit neskor (catch-up).
2. Po spusteni app scheduler dobehne chybajuce reporty.
3. Over SMTP a recipients.

### 13.5 Print PDF sa toci a nic

1. Skus iny prehliadac alebo povol pop-up/download.
2. Skontroluj ci endpoint vracia PDF (network panel v browseri).
3. Pozri `logs\\app.log` na server error.

### 13.6 Update zlyhal

1. Zatvor beziaci `AirportApp.exe`.
2. Spusti `install_update.exe` znova.
3. Over, ze cielovy priecinok obsahuje `AirportApp.exe`.

### 13.7 Data po update "zmizli"

1. Pravdepodobne sa app spustila v inom priecinku s inou DB.
2. Skontroluj kde je pouzivane `airport_app.db`.
3. Obnov DB z `backups`.

## 14. Odporucany denny postup pre obsluhu

1. Prihlasit sa.
2. Evidovat predaje priebezne cez `New Sale`.
3. Kontrolovat `Sales List`.
4. Na konci smeny skontrolovat Daily report.
5. Pravidelne kontrolovat notifikacie a logs.

## 15. Kontakt pre podporu

Pri incidente priprav pre support:
1. screenshot chyby,
2. cas udalosti,
3. poslednych 50-100 riadkov z `logs\\app.log`,
4. informaciu, ci islo o lokalne alebo USB spustenie,
5. verziu EXE / datum update.
