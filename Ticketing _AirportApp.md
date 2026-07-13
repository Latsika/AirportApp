# Ticketing AirportApp - User Manual

Tento manual popisuje aktualne fungovanie AirportApp pre End Usera, Deputy a Admina.

## Aktualizacia 2026-06-17

Security release `release_2026-06-17_security` prinasa:

1. Per-install secret subor `airport_app.secret` namiesto pevneho Flask `SECRET_KEY`.
2. Docasny 15-minutovy rate limit pre neuspesne login pokusy.
3. Docasny 15-minutovy rate limit pre neuspesne resetovanie hesla cez security otazky.
4. Security odpovede sa ukladaju ako bcrypt hashe.
5. Existujuce plaintext security odpovede sa automaticky premigruju na hashe pri starte aplikacie.
6. Portable/frozen app aplikuje best-effort Windows ACL ochranu na runtime subory.
7. Aktualny release priecinok neobsahuje `airport_app.db`, aby sa neprepisali zakaznicke data.

## 1. Co je AirportApp

1. AirportApp je lokalna desktop aplikacia spustana cez `AirportApp.exe`.
2. Po spusteni sa zobrazi v prehliadaci na lokalnej adrese `http://127.0.0.1:<port>`.
3. Server pocuva iba na `127.0.0.1`, takze nie je standardne dostupny z inych PC vo firemnej sieti.
4. Na cielovom PC nie je potrebny Python.
5. Data su lokalne v SQLite databaze `airport_app.db`.

## 2. Subory a priecinky aplikacie

Hlavne subory:

```text
AirportApp.exe
airport_app.db
airport_app.secret
backups/
logs/
```

Vyznam:

- `AirportApp.exe`: samotna aplikacia.
- `airport_app.db`: hlavna databaza, obsahuje users, airlines, sales, reports, rewards a nastavenia.
- `airport_app.secret`: lokalny secret pre podpisovanie session cookies.
- `backups/`: automaticke zalohy DB.
- `logs/app.log`: technicke logy.
- `app_runtime.json`: docasny subor s portom bezacej appky.
- `crash.log`: vznikne iba pri kritickej chybe startu.

Zakladne prevadzkove pravidlo:

- `airport_app.db` je zakaznicka databaza,
- build, release ani update ju nesmie prepisat,
- updater moze DB zalohovat, ale vymiena iba `AirportApp.exe`,
- pri manualnej oprave DB sa aktualny subor najprv premenuje alebo skopiruje.

## 3. Spustenie aplikacie

1. Skopiruj aplikaciu na lokalny disk.
2. Dvojklik na `AirportApp.exe`.
3. Otvori sa login stranka v prehliadaci.
4. Pri starte app:
- inicializuje alebo migruje DB schemu,
- vytvori automaticku zalohu DB,
- vytvori alebo nacita `airport_app.secret`,
- premigruje stare plaintext security odpovede na hashe,
- pripravi logy a notifikacne kontroly.

## 4. Prihlasenie, registracia, reset hesla

### 4.1 Login

Login polia:

- `Name or Nickname`
- `Password`

Ak je databaza prazdna, vytvori sa default admin:

```text
Nickname: Admin
Password: 12345
```

Pri prvom prihlaseni je vynutena zmena hesla.

### 4.2 Rate limit

Po 5 neuspesnych login pokusoch v 15 minutach aplikacia docasne odmietne dalsie pokusy.

Dolezite:

- ucet sa tym natrvalo nezablokuje,
- po 15 minutach je mozne skusit login znova,
- uspesny login resetuje pocitadlo neuspesnych pokusov.

### 4.3 Sign Up

1. Pouzivatel vyplni meno, nickname, heslo a 3 security otazky.
2. Novy ucet caka na schvalenie.
3. Schvalit ho moze Admin alebo Deputy.

### 4.4 Forgot Password

1. Pouzivatel zada nickname.
2. Aplikacia zobrazi security otazky.
3. Po spravnych odpovediach si pouzivatel nastavi nove heslo.
4. Aj reset hesla ma 15-minutovy rate limit pri neuspesnych pokusoch.

Security odpovede su ulozene ako bcrypt hashe, nie ako citatelny text.

## 5. Role a opravnenia

### User

- predaj,
- dostupne reporty podla menu,
- vlastny profil a zmena hesla.

### Deputy

- schvalovanie novych pouzivatelov,
- standardne pouzivatelske workflow.

### Admin

- plna sprava pouzivatelov,
- airlines, destinations, airline fees,
- airport service fees,
- sales edit/delete,
- reports,
- notifications,
- variable rewards,
- account settings,
- DB export.

## 6. Predaj

### 6.1 New Sale

1. Otvor `Sales -> New Sale`.
2. Vyber airline.
3. Vyber destination.
4. Volitelne zadaj:
- PNR,
- passenger name.
5. Vyber polozky:
- airline fees,
- airport service fees,
- plane ticket amount/quantity, ak sa pouziva.
6. Vyber platbu:
- `CASH`,
- `CARD`.
7. Uloz predaj.

Kazda polozka predaja sa uklada do `sale_items`.

### 6.1.1 Custom airline a custom destination

Ak zakaznik chce letenku cez aerolinku alebo do destinacie, ktora nie je v master zozname, pouzi:

- `Custom airline`, ak aerolinka nie je v zozname `Airline`,
- `Custom destination`, ak destinacia nie je v zozname `Destination`.

Pouzivatel vyplni:

- custom airline name,
- airline code,
- custom destination / krajinu,
- city,
- airport code.

Pravidla:

- custom airline automaticky pouziva custom destination flow, pretoze airline a destination spolu suvisia,
- custom airline/custom destination su dostupne iba pre predaj letenky,
- k takemu predaju je mozne pridat existujuce `Airport Service Fees`,
- custom airline/custom destination sa nepouzivaju pre standalone `Airline Fees`,
- custom airline sa neuklada do master zoznamu aeroliniek,
- custom destination sa neuklada do master zoznamu destinacii aerolinky,
- tieto custom hodnoty ostavaju ulozene iba na danom predaji.

### 6.2 Sales List

1. Otvor `Sales -> Sales List`.
2. Filtrovanie je dostupne podla PNR, passenger name, destination, seller a textoveho hladania. Destination filter vyhladava aj custom destination, city a airport code.
3. `Edit` upravi predaj.
4. `Delete` je admin akcia.
5. Zmeny sa loguju do sales logov.

## 7. Airlines, destinations a fees

Admin spravuje:

- Airlines,
- Airline destinations,
- Airline fees,
- Airport service fees.

Zmena ceny fee neprepise historicke predaje. Historicke zaznamy ostanu s povodnou cenou, nove predaje pouziju aktualnu cenu.

## 8. Reporty

### 8.1 Typy reportov

- Daily Report
- Monthly Report
- Custom Report

### 8.2 Exporty

Reporty sa daju exportovat do PDF a CSV, podla konkretnej obrazovky.

Download podporuje mena s diakritikou a Unicode znakmi. Aplikacia pouziva bezpecny ASCII fallback a UTF-8 `filename*` HTTP hlavicku.

### 8.3 Custom Report

Custom report podporuje filtre:

- date from / date to,
- airline / airport fees,
- destination,
- service/fee,
- sold by,
- payment method.

Spravanie:

- ak su vybrane iba airline fees, report zobrazi iba airline cast,
- ak su vybrane airline aj airport fees, report zobrazi kombinovane totaly,
- destination sa v detailoch zobrazuje ako kod, napr. `KSC`, `BTS`.
- custom airline/custom destination predaje su v samostatnej tabulke `Custom Destinations`,
- tabulka `Custom Destinations` zobrazuje airline, airline code, destination/krajinu, city, airport code, ticket qty/total, Airport Service Fee qty/total, cash a card totaly.

## 9. Variable Rewards

1. Rewards su naviazane na airport service fees za vybrany mesiac.
2. Admin moze nastavit:
- aktivny/neaktivny user pre rewards,
- globalne percento,
- manualnu sumu pre konkretneho usera.
3. `Save` uklada snapshot ako audit/historiu.
4. Obrazovky a PDF exporty pocitaju z aktualnych live DB hodnot.
5. Dostupne exporty:
- PDF pre vsetkych,
- PDF pre jedneho usera,
- yearly summary PDF za rozsah mesiacov.

## 10. Users a security administracia

Admin/Deputy:

- schvalenie cakajucich uctov.

Admin:

- edit usera,
- delete usera,
- reset hesla,
- reset security otazok,
- user logs,
- reassign admin.

Reset security otazok ulozi nove odpovede ako bcrypt hashe.

## 11. Notifications a SMTP

### 11.1 Recipients

V `Notifications` je mozne nastavit max 10 prijemcov.

### 11.2 Templates

Notification templates sa daju vytvarat, upravovat a zapinat/vypinat.

### 11.3 SMTP

SMTP je v `Account settings`.

Polia:

- SMTP host,
- SMTP port,
- SMTP user,
- SMTP password,
- sender,
- TLS.

SMTP heslo je ulozene v aplikacnej DB. Preto treba chranit `airport_app.db` a neposielat ju zbytocne mimo firmy.

### 11.4 Nastavenie mailov u zakaznika cez localhost

Ak aplikacia bezi u zakaznika ako lokalna portable app cez `http://127.0.0.1:<port>`, SMTP sa nastavuje stale v samotnej aplikacii. `localhost` je iba adresa pre browser; odosielanie mailov ide z PC zakaznika von na SMTP server.

Postup:

1. Spustit zakaznikov `AirportApp.exe`.
2. Prihlasit sa ako Admin.
3. Otvorit `Account settings`.
4. Vyplnit SMTP:
   - SMTP host,
   - SMTP port,
   - SMTP user,
   - SMTP password,
   - sender,
   - TLS.
5. Ulozit SMTP.
6. Otvorit `Create notifications`.
7. Doplnit aspon jednu prijemcovsku emailovu adresu.
8. Ulozit notification emails.
9. Ako rychly test otvorit `Reports` -> `Daily Report` a kliknut `SAVE`. To vytvori report-created notifikaciu a overi SMTP/prijemcov.

Dolezite SMTP pravidla:

- `Use TLS` znamena STARTTLS, typicky port `587`.
- Pre plain SMTP alebo lokalny test server treba TLS vypnut.
- Implicit SSL SMTP na porte `465` aktualny sender nepouziva.
- Pri Gmail, Microsoft 365 alebo firemnom SMTP moze byt potrebne app password alebo povolene authenticated SMTP.
- `sender` musi byt casto rovnaky ako SMTP user alebo povoleny alias.
- Firemny firewall/antivirus musi povolit odchod na SMTP host/port.

## 12. Automaticke report emaily

1. Scheduler bezi pri aktivite aplikacie.
2. Daily report sa posiela po `00:05` lokalneho casu za predchadzajuci den.
3. Monthly report sa posiela po `00:05` lokalneho casu za predchadzajuci mesiac.
4. Ak app nebezala, catch-up mechanizmus doposle chybajuce reporty po dalsom spusteni/pouziti.
5. Duplicity sa blokuju cez snapshot/app state kluce.
6. Automaticke daily/monthly report emaily maju PDF prilohu.

## 13. Account Settings

Admin ma dostupne:

- SMTP konfiguraciu,
- notification nastavenia,
- DB export.

DB export stiahne aktualny `airport_app.db`.

## 14. Prenos na nove PC

Na prenos existujucej instalacie kopiruj cely priecinok aplikacie.

Minimalne:

```text
AirportApp.exe
airport_app.db
airport_app.secret
backups/
```

Ak sa prenesie iba `AirportApp.exe`, aplikacia moze vytvorit novu prazdnu DB a povodne ucty nebudu dostupne.

Odporucanie:

1. Zatvor AirportApp.
2. Skopiruj cely priecinok na USB.
3. Na novom PC ho skopiruj z USB na lokalny disk.
4. Spust `AirportApp.exe` z lokalneho disku.

## 15. Update bez straty dat

Na existujuceho zakaznika pouzi:

```text
install_update.exe
```

Postup:

1. Spusti `install_update.exe`.
2. Vyber priecinok, kde je existujuci `AirportApp.exe`.
3. Updater zastavi beziacu appku.
4. Updater spravi backup `airport_app.db`.
5. Updater nahradi iba `AirportApp.exe`.
6. `airport_app.db`, `airport_app.secret` a `backups/` ostavaju v zakaznickom priecinku.

Neprepisuj zakaznikovu databazu release/test databazou.

## 16. Build a release pre developera

Portable build:

```powershell
installer\build_portable.bat
```

Updater build:

```powershell
installer\build_update.bat
```

Vystupy:

```text
dist/AirportApp.exe
dist/install_update.exe
```

Aktualny release:

```text
release_2026-06-17_security/
  AirportApp.exe
  install_update.exe
  RELEASE_NOTES.txt
```

Release priecinok nema obsahovat `airport_app.db`, pokial nejde vyslovene o demo/fresh install balik.

## 17. Troubleshooting

### Invalid credentials

1. Skontroluj nickname/full name.
2. Skontroluj Caps Lock a klavesnicu.
3. Skontroluj, ci sa spusta appka v priecinku so spravnou `airport_app.db`.
4. Pri novej prazdnej DB skus `Admin / 12345`.
5. Pouzi `Forgot Password` alebo Admin reset hesla.

### Too many failed login attempts

1. Pockaj 15 minut.
2. Skus znova so spravnym menom/heslom.
3. Ak heslo nie je zname, pouzi `Forgot Password` alebo Admin reset.

### App sa nespusti

1. Pozri `crash.log`.
2. Pozri `logs/app.log`.
3. Skontroluj prava na zapis do priecinka.
4. Skopiruj aplikaciu z USB na lokalny disk.
5. Over antivirus/firemne politiky.

### Updater zlyha

1. Zatvor AirportApp.
2. Over Task Manager, ci nebezi `AirportApp.exe`.
3. Spusti updater s pravom zapisovat do cieloveho priecinka.
4. Vyber spravny priecinok s `AirportApp.exe`.

### Emaily sa neposielaju

1. Skontroluj SMTP.
2. Skontroluj recipients.
3. Over firewall/proxy.
4. Pozri `logs/app.log`.

## 18. Prevadzka

1. Pravidelne zalohuj `airport_app.db` a `backups/` mimo zariadenia.
2. Pred update zatvor aplikaciu.
3. Produkcnu aplikaciu nespustaj dlhodobo z USB.
4. Pri presune kopiruj cely priecinok.
5. Neposielaj hesla cez email alebo chat.
