import re
import sqlite3
import time
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup


st.markdown("<h1 style='text-align: center; color: #0B3D91;'>MY DATA APP</h1>", unsafe_allow_html=True)
st.markdown(
    """
    <style>
    div[data-testid="stDataFrame"] thead tr th {
        background-color: #0B3D91 !important;
        color: white !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "AutoDakar.db"
RAW_DIR = BASE_DIR / "data" / "webscraper_raw"

menu = st.sidebar.radio(
    "Menu",
    [
        "Scraper (BeautifulSoup)",
        "Telecharger RAW (Web Scraper)",
        "Dashboard (Web Scraper clean)",
        "Evaluation",
    ],
)


if menu == "Scraper (BeautifulSoup)":
    st.subheader("Scraper Dakar-Auto avec BeautifulSoup")

    categorie = st.selectbox("Categorie", ["voitures", "motos", "location"])
    nb_pages = st.number_input("Nombre de pages", min_value=1, max_value=100, value=3, step=1)

    if st.button("Lancer le scraping"):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        try:
            c.execute(
                """CREATE TABLE AD_table(categorie, marque, annee, prix, adresse, kilometrage, boite_vitesse, carburant, proprietaire, page, url)"""
            )
            conn.commit()
        except:
            pass

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        if categorie == "voitures":
            base_url = "https://dakar-auto.com/senegal/voitures-4"
        elif categorie == "motos":
            base_url = "https://dakar-auto.com/senegal/motos-and-scooters-3"
        else:
            base_url = "https://dakar-auto.com/senegal/location-de-voitures-19"

        df = pd.DataFrame()

        for i in range(1, nb_pages + 1):
            url = f"{base_url}?page={i}"

            res = requests.get(url, headers=headers, timeout=30)
            soup = BeautifulSoup(res.content, "lxml")
            time.sleep(1)

            containers = soup.select("h2 a[href]")
            data = []

            for container in containers:
                try:
                    titre = container.get_text(strip=True)

                    if re.search(r"(19|20)\d{2}", titre) is None:
                        continue

                    h2 = container.find_parent("h2")
                    if h2 is None:
                        continue

                    marque = titre.split()[0]
                    annee = int(re.search(r"(19|20)\d{2}", titre).group())

                    prix = None
                    prix_tag = h2.find_next("h3")
                    if prix_tag is not None:
                        prev_h2 = prix_tag.find_previous("h2")
                        if (prev_h2 is not None) and (prev_h2.get_text(" ", strip=True) == h2.get_text(" ", strip=True)):
                            prix_txt = prix_tag.get_text(" ", strip=True)
                            if "CFA" in prix_txt:
                                prix = int(re.sub(r"\D", "", prix_txt))

                    adresse = None
                    if prix_tag is not None:
                        addr_node = prix_tag.find_next(string=re.compile(r","))
                        if addr_node is not None:
                            prev_h3 = addr_node.find_previous("h3")
                            if (prev_h3 is not None) and (prev_h3.get_text(" ", strip=True) == prix_tag.get_text(" ", strip=True)):
                                adr_txt = addr_node.strip()
                                if ("," in adr_txt) and (len(adr_txt) <= 60):
                                    adresse = adr_txt

                    proprietaire = None
                    bloc_owner = h2
                    txt_owner_best = None
                    best_len = 10**9

                    for j in range(1, 12):
                        if bloc_owner is None:
                            break
                        bloc_owner = bloc_owner.parent
                        if bloc_owner is None:
                            break

                        txt_tmp = bloc_owner.get_text("\n", strip=True)

                        if (titre in txt_tmp) and ("CFA" in txt_tmp) and ("Par " in txt_tmp):
                            if len(txt_tmp) < best_len:
                                txt_owner_best = txt_tmp
                                best_len = len(txt_tmp)

                    if txt_owner_best is not None:
                        lines_owner = [l.strip() for l in txt_owner_best.split("\n") if l.strip()]
                        proprietaire_raw = next((l for l in lines_owner if l.startswith("Par ")), None)
                        if proprietaire_raw:
                            proprietaire = proprietaire_raw.replace("Par ", "").strip()

                    if categorie == "voitures":
                        kilometrage = None
                        boite_vitesse = None
                        carburant = None

                        if prix_tag is not None:
                            ul = prix_tag.find_next("ul")
                            if ul is not None:
                                prev_h3_ul = ul.find_previous("h3")
                                if (prev_h3_ul is not None) and (prev_h3_ul.get_text(" ", strip=True) == prix_tag.get_text(" ", strip=True)):
                                    lis = ul.find_all("li")

                                    km_line = next((li.get_text(" ", strip=True) for li in lis if "km" in li.get_text().lower()), None)
                                    if km_line:
                                        kilometrage = int(re.sub(r"\D", "", km_line))
                                        if kilometrage == 1:
                                            kilometrage = None

                                    if any("Automatique" in li.get_text() for li in lis):
                                        boite_vitesse = "Automatique"
                                    elif any("Manuelle" in li.get_text() for li in lis):
                                        boite_vitesse = "Manuelle"

                                    if any("Diesel" in li.get_text() for li in lis):
                                        carburant = "Diesel"
                                    elif any("Essence" in li.get_text() for li in lis):
                                        carburant = "Essence"
                                    elif any(("Électrique" in li.get_text()) or ("Electrique" in li.get_text()) for li in lis):
                                        carburant = "Électrique"
                                    elif any("Hybride" in li.get_text() for li in lis):
                                        carburant = "Hybrides"

                        dic = {
                            "categorie": "voitures",
                            "marque": marque,
                            "annee": annee,
                            "prix": prix,
                            "adresse": adresse,
                            "kilometrage": kilometrage,
                            "boite_vitesse": boite_vitesse,
                            "carburant": carburant,
                            "proprietaire": proprietaire,
                            "page": i,
                            "url": url,
                        }
                        data.append(dic)

                        c.execute(
                            """INSERT INTO AD_table VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                            ("voitures", marque, annee, prix, adresse, kilometrage, boite_vitesse, carburant, proprietaire, i, url),
                        )
                        conn.commit()

                    elif categorie == "motos":
                        kilometrage = None
                        if prix_tag is not None:
                            ul = prix_tag.find_next("ul")
                            if ul is not None:
                                prev_h3_ul = ul.find_previous("h3")
                                if (prev_h3_ul is not None) and (prev_h3_ul.get_text(" ", strip=True) == prix_tag.get_text(" ", strip=True)):
                                    lis = ul.find_all("li")
                                    km_line = next((li.get_text(" ", strip=True) for li in lis if "km" in li.get_text().lower()), None)
                                    if km_line:
                                        kilometrage = int(re.sub(r"\D", "", km_line))
                                        if kilometrage == 1:
                                            kilometrage = None

                        dic = {
                            "categorie": "motos",
                            "marque": marque,
                            "annee": annee,
                            "prix": prix,
                            "adresse": adresse,
                            "kilometrage": kilometrage,
                            "boite_vitesse": None,
                            "carburant": None,
                            "proprietaire": proprietaire,
                            "page": i,
                            "url": url,
                        }
                        data.append(dic)

                        c.execute(
                            """INSERT INTO AD_table VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                            ("motos", marque, annee, prix, adresse, kilometrage, None, None, proprietaire, i, url),
                        )
                        conn.commit()

                    else:
                        dic = {
                            "categorie": "location",
                            "marque": marque,
                            "annee": annee,
                            "prix": prix,
                            "adresse": adresse,
                            "kilometrage": None,
                            "boite_vitesse": None,
                            "carburant": None,
                            "proprietaire": proprietaire,
                            "page": i,
                            "url": url,
                        }
                        data.append(dic)

                        c.execute(
                            """INSERT INTO AD_table VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                            ("location", marque, annee, prix, adresse, None, None, None, proprietaire, i, url),
                        )
                        conn.commit()

                except:
                    pass

            DF = pd.DataFrame(data)
            df = pd.concat([df, DF], axis=0).reset_index(drop=True)

        df["prix"] = df["prix"].astype("Int64")
        df["annee"] = df["annee"].astype("Int64")
        if "kilometrage" in df.columns:
            df["kilometrage"] = df["kilometrage"].astype("Int64")

        conn.close()

        if categorie == "motos":
            st.dataframe(df.head().drop(columns=["boite_vitesse", "carburant"], errors="ignore"))
        elif categorie == "location":
            st.dataframe(df.head().drop(columns=["kilometrage", "boite_vitesse", "carburant"], errors="ignore"))
        else:
            st.dataframe(df.head())
        st.write(df.shape)
        csv_data = df.to_csv(index=False).encode("utf-8")
        st.download_button("Telecharger CSV", data=csv_data, file_name=f"dakar_auto_{categorie}.csv", mime="text/csv")


elif menu == "Telecharger RAW (Web Scraper)":
    st.subheader("Telecharger les CSV RAW Web Scraper")
    st.write("Place les fichiers dans : data/webscraper_raw/")

    file_map = {
        "url 1.csv": RAW_DIR / "url 1.csv",
        "url 2.csv": RAW_DIR / "url 2.csv",
        "url 3.csv": RAW_DIR / "url 3.csv",
    }

    for name in file_map:
        path = file_map[name]
        if not path.exists() and (BASE_DIR / name).exists():
            path = BASE_DIR / name

        if path.exists():
            content = path.read_bytes()
            st.download_button(f"Telecharger {name}", data=content, file_name=name, mime="text/csv", key=f"dl_{name}")
            if st.button(f"Afficher apercu {name}", key=f"preview_{name}"):
                try:
                    df_preview = pd.read_csv(path)
                    st.dataframe(df_preview.head())
                except:
                    st.warning(f"Impossible de lire {name}")
        else:
            st.warning(f"Fichier introuvable : {path}")


elif menu == "Dashboard (Web Scraper clean)":
    st.subheader("Dashboard des donnees Web Scraper nettoyees")

    path1 = RAW_DIR / "url 1.csv"
    path2 = RAW_DIR / "url 2.csv"
    path3 = RAW_DIR / "url 3.csv"

    if not path1.exists() and (BASE_DIR / "url 1.csv").exists():
        path1 = BASE_DIR / "url 1.csv"
    if not path2.exists() and (BASE_DIR / "url 2.csv").exists():
        path2 = BASE_DIR / "url 2.csv"
    if not path3.exists() and (BASE_DIR / "url 3.csv").exists():
        path3 = BASE_DIR / "url 3.csv"

    if (not path1.exists()) or (not path2.exists()) or (not path3.exists()):
        st.warning("Fichiers RAW manquants. Place url 1.csv, url 2.csv, url 3.csv dans data/webscraper_raw/")
    else:
        df_clean = pd.DataFrame()

        df_raw1 = pd.read_csv(path1)
        df_raw1.columns = [c.strip() for c in df_raw1.columns]
        data = []
        for k in range(df_raw1.shape[0]):
            try:
                titre = ""
                if "marque" in df_raw1.columns:
                    titre = str(df_raw1.loc[k, "marque"])

                marque = None
                if len(titre.split()) > 0:
                    marque = titre.split()[0]

                annee = None
                m = re.search(r"(19|20)\d{2}", titre)
                if m:
                    annee = int(m.group())

                prix_source = None
                if "prix" in df_raw1.columns:
                    prix_source = str(df_raw1.loc[k, "prix"])
                prix = None
                if prix_source and prix_source != "nan":
                    prix_digits = re.sub(r"\D", "", prix_source)
                    if prix_digits != "":
                        prix = int(prix_digits)

                km_source = None
                if "kilométrage" in df_raw1.columns:
                    km_source = str(df_raw1.loc[k, "kilométrage"])
                elif "kilom?trage" in df_raw1.columns:
                    km_source = str(df_raw1.loc[k, "kilom?trage"])
                elif "kilometrage" in df_raw1.columns:
                    km_source = str(df_raw1.loc[k, "kilometrage"])

                kilometrage = None
                if km_source and km_source != "nan":
                    km_digits = re.sub(r"\D", "", km_source)
                    if km_digits != "":
                        kilometrage = int(km_digits)
                if kilometrage == 1:
                    kilometrage = None

                boite_vitesse = None
                if "boite vitesse" in df_raw1.columns:
                    boite_vitesse = str(df_raw1.loc[k, "boite vitesse"])
                    if boite_vitesse == "nan":
                        boite_vitesse = None

                carburant = None
                if "carburant" in df_raw1.columns:
                    carburant = str(df_raw1.loc[k, "carburant"])
                    if carburant == "nan":
                        carburant = None

                proprietaire = None
                if "propriétaire" in df_raw1.columns:
                    proprietaire = str(df_raw1.loc[k, "propriétaire"])
                elif "propri?taire" in df_raw1.columns:
                    proprietaire = str(df_raw1.loc[k, "propri?taire"])
                elif "proprietaire" in df_raw1.columns:
                    proprietaire = str(df_raw1.loc[k, "proprietaire"])
                if proprietaire and proprietaire != "nan":
                    proprietaire = proprietaire.replace("Par ", "").strip()
                else:
                    proprietaire = None

                adresse = None
                if "adresse" in df_raw1.columns:
                    adresse = str(df_raw1.loc[k, "adresse"])
                    if adresse == "nan":
                        adresse = None

                dic = {
                    "categorie": "voitures",
                    "marque": marque,
                    "annee": annee,
                    "prix": prix,
                    "adresse": adresse,
                    "kilometrage": kilometrage,
                    "boite_vitesse": boite_vitesse,
                    "carburant": carburant,
                    "proprietaire": proprietaire,
                }
                data.append(dic)
            except:
                pass

        DF = pd.DataFrame(data)
        df_clean = pd.concat([df_clean, DF], axis=0).reset_index(drop=True)

        df_raw2 = pd.read_csv(path2)
        df_raw2.columns = [c.strip() for c in df_raw2.columns]
        data = []
        for k in range(df_raw2.shape[0]):
            try:
                titre = ""
                if "marque" in df_raw2.columns:
                    titre = str(df_raw2.loc[k, "marque"])

                marque = None
                if len(titre.split()) > 0:
                    marque = titre.split()[0]

                annee = None
                m = re.search(r"(19|20)\d{2}", titre)
                if m:
                    annee = int(m.group())

                prix_source = None
                if "prix" in df_raw2.columns:
                    prix_source = str(df_raw2.loc[k, "prix"])
                prix = None
                if prix_source and prix_source != "nan":
                    prix_digits = re.sub(r"\D", "", prix_source)
                    if prix_digits != "":
                        prix = int(prix_digits)

                km_source = None
                if "kilométrage" in df_raw2.columns:
                    km_source = str(df_raw2.loc[k, "kilométrage"])
                elif "kilom?trage" in df_raw2.columns:
                    km_source = str(df_raw2.loc[k, "kilom?trage"])
                elif "kilometrage" in df_raw2.columns:
                    km_source = str(df_raw2.loc[k, "kilometrage"])

                kilometrage = None
                if km_source and km_source != "nan":
                    km_digits = re.sub(r"\D", "", km_source)
                    if km_digits != "":
                        kilometrage = int(km_digits)
                if kilometrage == 1:
                    kilometrage = None

                proprietaire = None
                if "propriétaire" in df_raw2.columns:
                    proprietaire = str(df_raw2.loc[k, "propriétaire"])
                elif "propri?taire" in df_raw2.columns:
                    proprietaire = str(df_raw2.loc[k, "propri?taire"])
                elif "proprietaire" in df_raw2.columns:
                    proprietaire = str(df_raw2.loc[k, "proprietaire"])
                if proprietaire and proprietaire != "nan":
                    proprietaire = proprietaire.replace("Par ", "").strip()
                else:
                    proprietaire = None

                adresse = None
                if "adresse" in df_raw2.columns:
                    adresse = str(df_raw2.loc[k, "adresse"])
                    if adresse == "nan":
                        adresse = None

                dic = {
                    "categorie": "motos",
                    "marque": marque,
                    "annee": annee,
                    "prix": prix,
                    "adresse": adresse,
                    "kilometrage": kilometrage,
                    "boite_vitesse": None,
                    "carburant": None,
                    "proprietaire": proprietaire,
                }
                data.append(dic)
            except:
                pass

        DF = pd.DataFrame(data)
        df_clean = pd.concat([df_clean, DF], axis=0).reset_index(drop=True)

        df_raw3 = pd.read_csv(path3)
        df_raw3.columns = [c.strip() for c in df_raw3.columns]
        data = []
        for k in range(df_raw3.shape[0]):
            try:
                titre = ""
                if "data" in df_raw3.columns:
                    titre = str(df_raw3.loc[k, "data"])

                marque = None
                if len(titre.split()) > 0:
                    marque = titre.split()[0]

                annee = None
                m = re.search(r"(19|20)\d{2}", titre)
                if m:
                    annee = int(m.group())

                prix_source = None
                if "price" in df_raw3.columns:
                    prix_source = str(df_raw3.loc[k, "price"])
                prix = None
                if prix_source and prix_source != "nan":
                    prix_digits = re.sub(r"\D", "", prix_source)
                    if prix_digits != "":
                        prix = int(prix_digits)

                proprietaire = None
                if "data2" in df_raw3.columns:
                    proprietaire = str(df_raw3.loc[k, "data2"])
                if proprietaire and proprietaire != "nan":
                    proprietaire = proprietaire.replace("Par ", "").strip()
                else:
                    proprietaire = None

                adresse = None
                if "data4" in df_raw3.columns:
                    adresse = str(df_raw3.loc[k, "data4"])
                    if adresse == "nan":
                        adresse = None

                dic = {
                    "categorie": "location",
                    "marque": marque,
                    "annee": annee,
                    "prix": prix,
                    "adresse": adresse,
                    "kilometrage": None,
                    "boite_vitesse": None,
                    "carburant": None,
                    "proprietaire": proprietaire,
                }
                data.append(dic)
            except:
                pass

        DF = pd.DataFrame(data)
        df_clean = pd.concat([df_clean, DF], axis=0).reset_index(drop=True)

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        try:
            c.execute(
                """CREATE TABLE WS_table(categorie, marque, annee, prix, adresse, kilometrage, boite_vitesse, carburant, proprietaire)"""
            )
            conn.commit()
        except:
            pass

        try:
            c.execute("""DELETE FROM WS_table""")
            conn.commit()
        except:
            pass

        for k in range(df_clean.shape[0]):
            try:
                c.execute(
                    """INSERT INTO WS_table VALUES(?,?,?,?,?,?,?,?,?)""",
                    (
                        df_clean.loc[k, "categorie"],
                        df_clean.loc[k, "marque"],
                        df_clean.loc[k, "annee"],
                        df_clean.loc[k, "prix"],
                        df_clean.loc[k, "adresse"],
                        df_clean.loc[k, "kilometrage"],
                        df_clean.loc[k, "boite_vitesse"],
                        df_clean.loc[k, "carburant"],
                        df_clean.loc[k, "proprietaire"],
                    ),
                )
                conn.commit()
            except:
                pass

        conn.close()

        st.write("Apercu du clean:")
        st.dataframe(df_clean.head())
        st.write(df_clean.shape)

        df_dash = df_clean.copy()

        cat_options = ["Toutes"] + sorted([x for x in df_dash["categorie"].dropna().unique().tolist()])
        cat_select = st.selectbox("Filtre categorie", cat_options)
        if cat_select != "Toutes":
            df_dash = df_dash[df_dash["categorie"] == cat_select].reset_index(drop=True)

        marque_options = ["Toutes"] + sorted([str(x) for x in df_dash["marque"].dropna().unique().tolist()])
        marque_select = st.selectbox("Filtre marque", marque_options)
        if marque_select != "Toutes":
            df_dash = df_dash[df_dash["marque"] == marque_select].reset_index(drop=True)

        annee_num = pd.to_numeric(df_dash["annee"], errors="coerce")
        if annee_num.dropna().shape[0] > 0:
            annee_min = int(annee_num.min())
            annee_max = int(annee_num.max())
            annee_range = st.slider("Filtre annee", annee_min, annee_max, (annee_min, annee_max))
            df_dash = df_dash[(pd.to_numeric(df_dash["annee"], errors="coerce") >= annee_range[0]) & (pd.to_numeric(df_dash["annee"], errors="coerce") <= annee_range[1])].reset_index(drop=True)

        prix_num = pd.to_numeric(df_dash["prix"], errors="coerce")
        if prix_num.dropna().shape[0] > 0:
            prix_min = int(prix_num.min())
            prix_max = int(prix_num.max())
            prix_range = st.slider("Filtre prix", prix_min, prix_max, (prix_min, prix_max))
            df_dash = df_dash[(pd.to_numeric(df_dash["prix"], errors="coerce") >= prix_range[0]) & (pd.to_numeric(df_dash["prix"], errors="coerce") <= prix_range[1])].reset_index(drop=True)

        prix_filtre = pd.to_numeric(df_dash["prix"], errors="coerce").dropna()
        nb_annonces = int(df_dash.shape[0])
        prix_moyen = int(prix_filtre.mean()) if prix_filtre.shape[0] > 0 else 0
        prix_median = int(prix_filtre.median()) if prix_filtre.shape[0] > 0 else 0

        col1, col2, col3 = st.columns(3)
        col1.metric("Nombre annonces", nb_annonces)
        col2.metric("Prix moyen", prix_moyen)
        col3.metric("Prix median", prix_median)

        st.write("Top 10 marques")
        top_marques = df_dash["marque"].fillna("NA").value_counts().head(10)
        st.bar_chart(top_marques)

        st.write("Distribution des prix")
        prix_dist = pd.to_numeric(df_dash["prix"], errors="coerce").dropna()
        if prix_dist.shape[0] > 0:
            bins = pd.cut(prix_dist, bins=10)
            dist = bins.value_counts().sort_index()
            dist_df = pd.DataFrame({"intervalle": dist.index.astype(str), "nb_annonces": dist.values})
            dist_df = dist_df.set_index("intervalle")
            st.bar_chart(dist_df)
        else:
            st.info("Pas de prix disponible pour la distribution.")

        st.write("Table filtree")
        st.dataframe(df_dash)


elif menu == "Evaluation":
    st.subheader("Évaluation de l'application")
    st.write("Merci de remplir ce formulaire :")
    st.markdown("[Ouvrir le formulaire KoboToolbox](https://ee.kobotoolbox.org/x/R3VhDSjj)")
