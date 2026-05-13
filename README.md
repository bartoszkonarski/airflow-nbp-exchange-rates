## Instrukcja instalacji
### Prerequisites:
- Docker (docker compose)

### Setup lokalnego środowiska:
1. Przygotowanie pliku środowiskowego .env (możliwy również ręczny setup)
```
cp .env.example .env
```
2. Zbudowanie obrazu na podstawie oficjalnego obrazu Airflow
```
docker compose build
```
3. Inicjalizacja bazy danych Airflow
```
docker compose up airflow-init
```
4. Uruchomienie środowiska
```
docker compose up -d
```

### Uruchomienie ETL dla danego zakresu czasu
```
docker compose exec airflow-worker airflow dags backfill \
    --start-date 2025-01-01 \
    --end-date 2025-01-10 \
    exchange_rates_dag
```
alternatywnie uruchomienie poprzez UI DAGa
## Podjęte Decyzje Architektoniczne Procesu

### Strategia Partycjonowania Danych (Hive Partitioning)
Zgodnie z wytycznymi zadania, dane w warstwie Silver są przygotowane pod integrację z usługą Google BigQuery jako External Table poprzez użycie schematu partycjonowania w stylu Hive:
```silver/nbp/exchange_rates_a/effective_date=YYYY-MM-DD/exchange_rates.parquet```

Wybór kolumny effective_date dokonany został ze względu na charakterystykę danych w postaci szeregu czasowego. Partcjonowanie takie pozwala na partition pruning w przypadku gdy zapytania analityczne filtrują dane po dacie (np. ```WHERE effective_date BETWEEN '2025-10-01' AND '2025-10-31'```). Dzięki temu, BigQuery ignoruje foldery spoza tego zakresu skutecznie zmniejszając wolumen skanowanych danych, tym samym koszt (w przeciwieństwie np. do Snowflake, częścią kosztu zapytania jest wolumen skanowanych danych) oraz czas odpowiedzi.

Partycjonowanie po logicznej dacie uruchomienia (ds) sprawia, że proces jest odporny na przetwarzanie danych przy nownych uruchomieniach dla tej samej daty oraz re-runach zakończonych już dag runów. Uruchomienie DAG-a dla określonego dnia nadpisuje zawartość tylko jednego jednoznacznie określonego katalogu partycji, co eliminuje ryzyko duplikacji danych i minimalizuje czas zapisu (szczególnie w przypadku magazynowych usług chmurowych).

### Wybór źródła danych NBP
Zadanie wskazywało stronę WWW jako źródło informacji o kursach walut. Podjąłem decyzję o wykorzystaniu zamiast tego oficjalnego publicznego API (https://api.nbp.pl/), o którym informacje znalazłem na wskazanej stronie NBP.

Dzięki temu mogłem dużo łatwiej i czytelniej pobrać dane z poziomu DAGa (wciąż w formacie XML, zgodnie z poleceniem), bez konieczności implementowania logiki wyboru odpowiedniej strony, pobierania pliku przyciskiem itp. 