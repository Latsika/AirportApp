# AirportApp Troubleshooting

Tento dokument popisuje najcastejsie problemy pri prenose, aktualizacii, spustani a prihlasovani v AirportApp.

## Najdolezitejsie pravidlo

Zakaznicke data su v subore:

```text
airport_app.db
```

Tento subor sa nesmie prepisat, zmazat ani nahradit buildom alebo release balikom. Pri zakaznikovi je `airport_app.db` hlavny zdroj dat.

Pravidlo:

- updater moze databazu zalohovat,
- updater nesmie databazu nahradit,
- release priecinok nema obsahovat zakaznicku ani testovaciu databazu,
- pred manualnou opravou sa aktualna DB najprv kopiruje alebo premenuje,
- ak si nie ste isti, nerobte overwrite DB.

Pri portable aplikacii musi byt databaza v tom istom priecinku ako:

```text
AirportApp.exe
```

Od security release `2026-06-17` aplikacia vytvara aj lokalny subor:

```text
airport_app.secret
```

Tento subor sluzi na podpisovanie prihlasovacich session cookies. Nie je to databaza, ale je bezpecnostne citlivy. Pri presune existujucej instalacie odporucame kopirovat cely priecinok aplikacie.

## Spravny prenos aplikacie cez USB

### Na povodnom pocitaci

1. Zatvorte AirportApp.
2. Najdite priecinok, kde je `AirportApp.exe`.
3. Skontrolujte, ze v rovnakom priecinku je `airport_app.db`.
4. Skopirujte cely priecinok aplikacie na USB.

Minimalne skopirujte:

```text
AirportApp.exe
airport_app.db
airport_app.secret
backups/
```

Odporucane je kopirovat cely priecinok:

```text
AirportApp/
  AirportApp.exe
  airport_app.db
  airport_app.secret
  backups/
  logs/
```

### Na novom pocitaci

1. Skopirujte cely priecinok z USB na lokalny disk, napriklad `Desktop` alebo `Documents`.
2. Spustite `AirportApp.exe` z lokalneho disku.
3. Nespustajte produkcnu appku dlhodobo priamo z USB.
4. Nevymazavajte ani nepresuvajte `airport_app.db` mimo priecinka aplikacie.

## Problem: po prenose sa neda prihlasit

### Priznaky

- Povodne meno alebo heslo nefunguje.
- Pouzivatelia alebo predaje chybaju.
- Funguje iba defaultny `Admin`, alebo aplikacia vyzera ako nova.

### Najpravdepodobnejsia pricina

Na novy pocitac nebola prenesena spravna databaza `airport_app.db`, alebo sa spusta iny `AirportApp.exe` v inom priecinku.

### Kontrola

V priecinku, odkial sa spusta `AirportApp.exe`, skontrolujte:

```text
AirportApp.exe
airport_app.db
```

Ak `airport_app.db` chyba, aplikacia nema povodne data.

Ak `airport_app.db` existuje, skontrolujte velkost:

- velmi mala databaza, napr. okolo `200-250 KB`, casto znamena prazdnu/startovaciu DB,
- vacsia databaza, napr. `800 KB+`, pravdepodobne obsahuje realne data,
- presna velkost zavisi od poctu predajov, pouzivatelov a reportov.

### Oprava

1. Zatvorte AirportApp na novom pocitaci.
2. Aktualny `airport_app.db` premenujte napriklad na:

```text
airport_app_old.db
```

3. Z povodneho pocitaca skopirujte spravny `airport_app.db`.
4. Vlozte ho do rovnakeho priecinka, kde je `AirportApp.exe`.
5. Spustite aplikaciu a skuste povodny ucet.

## Problem: Too many failed login attempts

Od security release `2026-06-17` aplikacia pouziva docasny rate limit.

Spravanie:

- po 5 neuspesnych login pokusoch v 15 minutach sa dalsie pokusy docasne odmietnu,
- blokovanie je docasne, ucet sa tym natrvalo nezablokuje,
- po uplynuti 15 minut je mozne skusit prihlasenie znova,
- uspesne prihlasenie resetuje pocitadlo neuspesnych pokusov.

Co robit:

1. Pockajte 15 minut.
2. Skontrolujte spravny nickname/full name.
3. Skontrolujte rozlozenie klavesnice a Caps Lock.
4. Ak heslo nie je zname, pouzite `Forgot Password` alebo Admin reset hesla.

## Problem: Too many failed password reset attempts

Aj `Forgot Password` ma docasny 15-minutovy rate limit.

Po viacerych nespravnych odpovediach na security otazky:

1. Pockajte 15 minut.
2. Skuste odpovede znova.
3. Ak odpovede nie su zname, Admin moze pouzit `Reset security questions`.

Security odpovede sa od release `2026-06-17` ukladaju ako bcrypt hashe. Starsie plaintext odpovede sa automaticky migruju na hashe pri starte aplikacie.

## Problem: airline alebo destination nie je v zozname pri predaji letenky

### Priznaky

- Zakaznik chce letenku cez aerolinku, ktora nie je v `Airline`.
- Zakaznik chce letenku do krajiny/mesta, ktore nie je v `Destination`.
- Aerolinka este nema danu destinaciu v master zozname.
- Predajca potrebuje predat letenku a zaroven evidovat statistiku pre buduce linky.

### Riesenie

V `Sales -> New Sale` vyber podla situacie:

```text
Custom airline
Custom destination
```

Ak sa vyberie `Custom airline`, aplikacia automaticky pouzije aj custom destination flow, pretoze airline a destination spolu suvisia.

Potom vypln:

- custom airline name,
- airline code,
- custom destination / krajinu,
- city,
- airport code.

Pravidla:

- musi ist o predaj letenky, teda `Plane Ticket Qty` a `Plane Ticket Price` musia byt vyplnene,
- je mozne pridat uz existujuce `Airport Service Fees`,
- nie je mozne pouzit custom airline/custom destination pre standalone `Airline Fees`,
- custom airline sa neprida do master zoznamu aeroliniek,
- custom destination sa neprida do master zoznamu destinacii,
- custom hodnoty ostavaju ulozene iba na konkretnom predaji.

### Kde najst statistiku

Otvor `Reports -> Custom Report`.

Ak existuju predaje s custom airline alebo custom destinaciou, report zobrazi tabulku:

```text
Custom Destinations
```

Tabulka obsahuje airline, airline code, destination/krajinu, city, airport code, ticket qty/total, Airport Service Fee qty/total, cash a card totaly.

## Aktualizacia aplikacie

Na aktualizaciu existujuceho zakaznika pouzite:

```text
install_update.exe
```

Spravny postup:

1. Zatvorte AirportApp, ak bezi.
2. Spustite `install_update.exe`.
3. Vyberte priecinok, kde je zakaznikov `AirportApp.exe`.
4. Updater zalohuje `airport_app.db`.
5. Updater vymeni iba `AirportApp.exe`.
6. Data ostanu zachovane.

Nepouzivajte cerstvy release priecinok ako nahradu celej zakaznickej instalacie, ak zakaznik uz ma data. Release priecinok `release_2026-06-17_security` zamerne neobsahuje `airport_app.db`.

## Cista instalacia vs aktualizacia

### Cista instalacia

Pouzite len vtedy, ked zakaznik nema existujuce data.

Pri prvom starte sa vytvori:

```text
airport_app.db
airport_app.secret
backups/
logs/
```

Pri prazdnej databaze sa vytvori default admin:

```text
Nickname: Admin
Password: 12345
```

Pouzivatel musi heslo zmenit.

### Aktualizacia existujuceho zakaznika

Pri existujucom zakaznikovi sa nema prepisovat:

```text
airport_app.db
airport_app.secret
backups/
```

Aktualizuje sa iba:

```text
AirportApp.exe
```

## Obnova z backupu

Zalohy su v priecinku:

```text
backups/
```

Backup subory maju nazvy podobne:

```text
airport_app_2026-06-04_093711.db
airport_app_update_2026-06-17_104500.db
```

Postup obnovy:

1. Zatvorte AirportApp.
2. Aktualny `airport_app.db` premenujte napriklad na `airport_app_before_restore.db`.
3. Vybrany backup skopirujte do priecinka aplikacie.
4. Premenujte backup na `airport_app.db`.
5. Spustite aplikaciu.

## Problem: aplikacia sa nespusti

1. Skontrolujte `crash.log` v priecinku aplikacie.
2. Skontrolujte `logs/app.log`.
3. Overte, ze priecinok nie je read-only.
4. Skopirujte aplikaciu z USB na lokalny disk a spustite ju odtial.
5. Overte, ze antivirus alebo firemna politika neblokuje `AirportApp.exe`.

## Problem: updater zlyha

1. Zatvorte AirportApp.
2. Overte, ze nebezi stary `AirportApp.exe` v Task Manageri.
3. Spustite `install_update.exe` ako pouzivatel s pravom zapisovat do cieloveho priecinka.
4. Vyberte priecinok, kde je existujuci `AirportApp.exe`.
5. Ak antivirus blokuje prepis `.exe`, povolte vynimku alebo poziadajte IT.

## Problem: emaily alebo reporty nechodia

1. Skontrolujte SMTP host, port, user, password a TLS.
2. Skontrolujte notification recipients.
3. Overte internet/firewall pre SMTP port.
4. Pozrite `logs/app.log`.
5. Automaticke reporty sa posielaju pri aktivite aplikacie; ak app nebezala, catch-up prebehne po dalsom spusteni/pouziti.

## Bezpecny postup pred opravou DB

Pred kazdym kopirovanim alebo prepisovanim databazy:

1. Zatvorte AirportApp.
2. Spravte kopiu aktualneho `airport_app.db`.
3. Premenujte povodny subor namiesto vymazania.
4. Az potom vlozte inu databazu.

Priklad:

```text
airport_app.db -> airport_app_before_fix.db
```

## Rychly checklist

1. Je `airport_app.db` v rovnakom priecinku ako `AirportApp.exe`?
2. Spusta zakaznik spravny `AirportApp.exe`?
3. Nebol presunuty iba samotny `.exe`?
4. Nie je databaza podozrivo mala alebo prazdna?
5. Existuje `backups/` so starsimi zalohami?
6. Nie je login docasne zablokovany rate limitom?
7. Nebola zakaznicka DB prepisana release/test DB?

## Co poslat developerovi pri podpore

Poslite:

- screenshot priecinka, kde je `AirportApp.exe`,
- velkost `airport_app.db`,
- informaciu, ci existuje `airport_app.secret`,
- informaciu, ci existuje `backups/`,
- najnovsi relevantny obsah z `logs/app.log` alebo `crash.log`,
- popis, ci sa prenasal cely priecinok alebo iba `.exe`,
- presny text chyby z obrazovky.

Neposielajte hesla pouzivatelov.
