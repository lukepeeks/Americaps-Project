import pandas as pd
from datetime import timedelta

# Inlezen van het originele bestand.
df = pd.read_csv("Beer.csv")

def clean_beer_data():
    print("Start data cleaning voor Beer.csv")

#Hier worden de dubbele rijen verwijderd uit het bestand.
    df.drop_duplicates(inplace=True)

 #Hier wordt de Audit controle aangepast, ongeldige termen worden verwijderd.
    allowed_audit = {"Goedgekeurd", "Afgekeurd", "Herziening vereist"}
    audit_invalid = df[~df["Audit van Leverancier"].isin(allowed_audit)]
    if audit_invalid.empty:
        print("Auditstatus: alle waarden zijn geldig.")
    else:
        print("⚠ Ongeldige waarden in Audit van Leverancier:")
        print(audit_invalid)

#Hier worden ontbrekende tijden of onlogische tijden aangepast naar een gemiddelde.
    def repair_timestamps(start_col, end_col, label):
        df[start_col] = pd.to_datetime(df[start_col], errors="coerce")
        df[end_col] = pd.to_datetime(df[end_col], errors="coerce")
        delta = (df[end_col] - df[start_col]).dt.total_seconds() / 3600
        average_hours = delta.dropna().mean()
        print(f"Gemiddelde {label}-tijd: {round(average_hours, 2)} uur")

        for i, row in df.iterrows():
            if pd.isna(row[start_col]) and pd.notna(row[end_col]):
                df.at[i, start_col] = row[end_col] - timedelta(hours=average_hours)
            elif pd.isna(row[end_col]) and pd.notna(row[start_col]):
                df.at[i, end_col] = row[start_col] + timedelta(hours=average_hours)

        df[start_col] = df[start_col].dt.strftime('%Y-%m-%d %H:%M:%S')
        df[end_col] = df[end_col].dt.strftime('%Y-%m-%d %H:%M:%S')

    repair_timestamps("GrindingDatumTijdStart", "GrindingDatumTijdEind", "Grinding")
    repair_timestamps("FillingDatumTijdStart", "FillingDatumTijdEind", "Filling")
    repair_timestamps("PackagingDatumTijdStart", "PackagingDatumTijdEind", "Packaging")

#Zelfde geval als hierboven, ontbrekende of onlogische tijden worden aangepast naar een gemiddelde.
    df["Cyclustijd"] = df["Cyclustijd"].astype(str).str.replace(" uur", "")
    df["Cyclustijd"] = pd.to_numeric(df["Cyclustijd"], errors="coerce")
    cycle_mean = df["Cyclustijd"].loc[(df["Cyclustijd"] > 0) & (df["Cyclustijd"] < 100)].mean()
    df["Cyclustijd"] = df["Cyclustijd"].fillna(cycle_mean).round(2).astype(str) + " uur"
    print(f"Cyclustijd gestandaardiseerd (gemiddelde: {round(cycle_mean, 2)} uur)")

 # Codes als XXXX en negatieve getallen worden vervangen door nieuwe unieke nummers, zoals hoort bij een ID.
    df["GrindingID"] = df["GrindingID"].astype(str)
    invalid_ids = df["GrindingID"].isin(["XXXX"]) | df["GrindingID"].str.startswith("-")
    available_ids = set(map(str, range(1, 8002))) - set(df.loc[~invalid_ids, "GrindingID"])
    new_ids = iter(sorted(available_ids))
    df.loc[invalid_ids, "GrindingID"] = [next(new_ids) for _ in range(invalid_ids.sum())]
    df["GrindingID"] = df["GrindingID"].astype(int)
    print("GrindingID's zijn opgeschoond")

 #In zowel Cost als Energieverbruik stonde ongeldige waarden, deze zijn gecontroleerd en indien nodig aangepast naar een gemiddelde.
    df["Cost"] = df["Cost"].astype(str).str.replace(" euros", "").str.strip()
    df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce")
    mean_cost = df["Cost"].loc[(df["Cost"] >= 0) & (df["Cost"] <= 10000)].mean()
    df["Cost"] = df["Cost"].fillna(mean_cost).round(2).astype(str) + " euros"
    print(f"Gemiddelde kostprijs: {round(mean_cost, 2)} euros")

 # Zie hierboven de uitleg.
    df["Energieverbruik"] = df["Energieverbruik"].astype(str).str.replace(" kWh", "")
    df["Energieverbruik"] = pd.to_numeric(df["Energieverbruik"], errors="coerce")
    valid_energy = df["Energieverbruik"].between(0, 1000)
    mean_energy = df.loc[valid_energy, "Energieverbruik"].mean()
    df["Energieverbruik"] = df["Energieverbruik"].where(valid_energy, mean_energy).round(2).astype(str) + " kWh"
    print(f"Energieverbruik aangepast op basis van gemiddelde: {round(mean_energy, 2)} kWh")

 # Bij zowel de duurzaamheid score, voorraadniveaus, gewichtcontrole en de klanttevredenheid stonden soms onlogische scores, soms vervangen door een gemiddelde en indien een gebrek aan gegevens is het als ongeldig bestempeld.
    df["Duurzaamheid Score"] = pd.to_numeric(df["Duurzaamheid Score"], errors="coerce")
    invalid_d = ~df["Duurzaamheid Score"].between(0, 100)
    df.loc[invalid_d, "Duurzaamheid Score"] = "Ongeldige Duurzaamheidsscore"
    print("Duurzaamheidsscore gecontroleerd")

 #Zie hierboven de uitleg.
    df["Klanttevredenheid"] = pd.to_numeric(df["Klanttevredenheid"], errors="coerce")
    klant_valid = df["Klanttevredenheid"].between(0, 10)
    gem_tevredenheid = df.loc[klant_valid, "Klanttevredenheid"].mean()
    df["Klanttevredenheid"] = df["Klanttevredenheid"].where(klant_valid, gem_tevredenheid).round(1)
    print(f"Klanttevredenheid opgeschoond met gemiddelde waarde van {round(gem_tevredenheid, 1)}")

 #Zie hierboven de uitleg
    df["Voorraadniveaus"] = pd.to_numeric(df["Voorraadniveaus"], errors="coerce")
    voorraad_valid = df["Voorraadniveaus"].between(0, 10000)
    gem_voorraad = df.loc[voorraad_valid, "Voorraadniveaus"].mean()
    df["Voorraadniveaus"] = df["Voorraadniveaus"].where(voorraad_valid, gem_voorraad).round(0).astype(int)
    print(f"Gemiddeld voorraadniveau ingesteld op: {round(gem_voorraad)}")

#Zie hierboven de uitleg
    df["Gewichtscontrole"] = pd.to_numeric(df["Gewichtscontrole"], errors="coerce")
    gewicht_valid = df["Gewichtscontrole"].between(0.001, 0.5)
    gem_gewicht = df.loc[gewicht_valid, "Gewichtscontrole"].mean()
    df["Gewichtscontrole"] = df["Gewichtscontrole"].where(gewicht_valid, gem_gewicht).round(3)
    print(f"Gemiddeld gewicht ingesteld op: {round(gem_gewicht, 3)} kg")

# Rijen waarin informatie elkaar tegenspreekt, zoals een defect product met een defectpercentage van 0% zijn gecontroleerd en verwijderd/aangepast waar nodig.
    if "Audit van Leverancier" in df.columns and "Aantal defecten" in df.columns:
        try:
            df["Aantal defecten"] = pd.to_numeric(df["Aantal defecten"], errors="coerce")
            tegenstrijdig = df[(df["Audit van Leverancier"] == "Afgekeurd") &
                               ((df["Aantal defecten"] == 0) | (df["Aantal defecten"].isna()))]
            if not tegenstrijdig.empty:
                print(f"Gevonden: {len(tegenstrijdig)} rijen met 'Afgekeurd' maar géén defecten geregistreerd.")
                df.loc[tegenstrijdig.index, "Controle op logica"] = "Afgekeurd zonder defect"
        except Exception as e:
            print(f"Fout bij controleren op tegenstrijdigheden: {e}")

#Hier wordt het opgeschoonde bestand opgeslagen.
    df.to_csv("Beer_cleaned.csv", index=False)
    print("Opgeschoonde data opgeslagen als Beer_cleaned.csv")

# Script draaien gebeurt hierzo.
if __name__ == "__main__":
    clean_beer_data()
