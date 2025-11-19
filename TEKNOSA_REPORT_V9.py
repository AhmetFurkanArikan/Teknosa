import pandas as pd
from datetime import datetime

# === DOSYA AYARLARI ===
FILE_PATH = "gfk_sales_202546_20251117050122.csv"         # Ana satış datası
GIFTCARD_FILE_PATH = "gfk_gift_card_20251117055556.csv"   # Gift card datası

# === ANA SATIŞ DATASI SÜTUN İSİMLERİ (kendi dosyana göre kontrol et) ===
QTY_COL = "Sipariş Miktarı"
REVENUE_COL = "KDV dahil ciro"
CATEGORY_COL = "Kategori2"
ORG_COL = "OrganizationCode"
BRAND_COL = "Marka"          # Marka sütunu adı
STORE_COL = "Magaza"         # Mağaza sütunu adı
PRODUCT_COL = "Uzun Tanım"   # Ürün açıklaması sütunu
CATEGORY3_COL = "Kategori3"   # Refurbished kırılımları için kullanılacak kategori


# === GIFTCARD DATASI SÜTUN İSİMLERİ ===
GC_PRODUCT_COL = "MALZEME TANIMI"   # Gift card ürün adı
GC_QTY_COL = "MIKTAR"               # Gift card miktar
GC_INVOICE_COL = "FATURA_TUTARI"    # Gift card fatura tutarı (brüt)
GC_DISC_COL = "INDIRIM_TUTARI"      # Gift card indirim tutarı

# Online mağaza isimleri
ONLINE_STORES = {
    "AMAZON",
    "HEPSIBURADA",
    "MP",
    "N11",
    "PAZARAMA",
    "TEKNOSA",
    "TEKNOSA KURUMSAL ELEKTRONIK",
    "TRENDYOL",
}


# === GİRİŞ / ÇIKIŞ MESAJLARI ===
def print_banner():
    print("\n==============================")
    print("     STATVISION'a Hoş Geldiniz")
    print("   Teknosa Veri Analiz Motoru")
    print("==============================\n")
    print("Program başlatılıyor...\n")


def print_goodbye():
    print("\n==============================")
    print("         STATVISION")
    print("   Raporlama işlemi tamamlandı")
    print("        İyi çalışmalar!")
    print("==============================\n")


def count_bad_lines(path: str) -> int:
    """
    Verilen CSV dosyasındaki bozuk satırları sayar.
    Yöntem: header'daki ; sayısını referans alıp her satırla karşılaştırmak.
    """
    bad = 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            header = f.readline().rstrip("\n")
            expected_cols = header.count(";") + 1

            for line in f:
                col_count = line.count(";") + 1
                if col_count != expected_cols:
                    bad += 1
    except FileNotFoundError:
        return 0

    return bad


def clean_numeric_column(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Sayısal kolonları akıllı şekilde temizler.
    - Eğer zaten numerik ise: sadece NaN -> 0
    - Eğer string ise:
        * Eğer çoğu değerde ',' varsa TR formatı varsayılır: 1.234,56 -> 1234.56
        * Sonra to_numeric uygulanır.
    """
    if col not in df.columns:
        return pd.Series(dtype="float64")

    s = df[col]

    # Zaten numerik ise:
    if pd.api.types.is_numeric_dtype(s):
        df[col] = s.fillna(0)
        return df[col]

    # String olarak al
    s_str = s.astype(str).str.strip()

    # Değerlerin ne kadarında ',' var? (TR formatını tespit için)
    comma_ratio = s_str.str.contains(",", regex=False).mean()

    if comma_ratio > 0.5:
        # Büyük ihtimalle TR formatı: 1.234,56
        # Önce binlik ayırıcı '.' kaldır, sonra ',' -> '.'
        s_str = s_str.str.replace(".", "", regex=False)
        s_str = s_str.str.replace(",", ".", regex=False)

    df[col] = pd.to_numeric(s_str, errors="coerce").fillna(0)
    return df[col]


def load_data(path: str) -> pd.DataFrame:
    """
    Ana satış datasını ; delimiter ile okur.
    Bozuk satırları atlar ve numerik kolonları hazırlar.
    """
    df = pd.read_csv(
        path,
        sep=";",
        encoding="utf-8",
        engine="python",
        on_bad_lines="skip"
    )

    # TSAMP → online olarak işaretle (çok büyük bir numeric değere dönüştür)
    df[ORG_COL] = df[ORG_COL].replace("TSAMP", "999999")

    # OrganizationCode'u numerik yap
    df[ORG_COL] = pd.to_numeric(df[ORG_COL], errors="coerce")

    # Mağaza isimlerini temizle
    if STORE_COL in df.columns:
        df[STORE_COL] = df[STORE_COL].astype(str).str.strip()

    # Adet & ciro kolonlarını temizle
    clean_numeric_column(df, QTY_COL)
    clean_numeric_column(df, REVENUE_COL)

    return df


def load_giftcard_data(path: str) -> pd.DataFrame:
    """
    Gift card datasını okur.
    Aynı delimiter ve sayısal temizleme mantığı kullanılır.
    """
    try:
        df = pd.read_csv(
            path,
            sep=";",
            encoding="utf-8",
            engine="python",
            on_bad_lines="skip"
        )
    except FileNotFoundError:
        print(f"\n⚠️ Gift card dosyası bulunamadı: {path}")
        return pd.DataFrame()

    # Numerik kolonları temizle
    clean_numeric_column(df, GC_QTY_COL)
    clean_numeric_column(df, GC_INVOICE_COL)
    clean_numeric_column(df, GC_DISC_COL)

    return df


def find_renewed_column(df: pd.DataFrame) -> str | None:
    """
    DataFrame içindeki sütunlar arasında 'YENILEN' kelimesini içeren
    ilk sütun adını döner. (Yenilenmiş ürün kolonu için otomatik tespit)
    Bulamazsa None döner.
    """
    for col in df.columns:
        col_upper = str(col).upper()
        if "YENILEN" in col_upper:   # YENILEN, YENİLENMİŞ vs. tüm varyasyonları yakalar
            return col
    return None


# === DATAFRAME ÜRETEN YARDIMCI FONKSİYONLAR (EXCEL İÇİN DE KULLANILACAK) ===

def get_total_df(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([{
        "Toplam_Adet": df[QTY_COL].sum(),
        "Toplam_Ciro": df[REVENUE_COL].sum()
    }])


def get_category_df(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(CATEGORY_COL)
          .agg(
              Toplam_Adet=(QTY_COL, "sum"),
              Toplam_Ciro=(REVENUE_COL, "sum")
          )
          .reset_index()
          .sort_values("Toplam_Ciro", ascending=False)
    )


def get_brand_top10_df(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(BRAND_COL)
          .agg(
              Toplam_Adet=(QTY_COL, "sum"),
              Toplam_Ciro=(REVENUE_COL, "sum")
          )
          .reset_index()
          .sort_values("Toplam_Ciro", ascending=False)
          .head(10)
    )

def get_brand_all_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ana datadaki TÜM markalar için adet ve ciro özetini döner.
    Ciroya göre büyükten küçüğe sıralanır.
    """
    return (
        df.groupby(BRAND_COL)
          .agg(
              Toplam_Adet=(QTY_COL, "sum"),
              Toplam_Ciro=(REVENUE_COL, "sum")
          )
          .reset_index()
          .sort_values("Toplam_Ciro", ascending=False)
    )

def get_store_online_offline_df(df: pd.DataFrame):
    mask_online = df[STORE_COL].isin(ONLINE_STORES)
    online_df = df[mask_online]
    offline_df = df[~mask_online]

    online_result = (
        online_df.groupby(STORE_COL)
                 .agg(
                     Toplam_Adet=(QTY_COL, "sum"),
                     Toplam_Ciro=(REVENUE_COL, "sum")
                 )
                 .reset_index()
                 .sort_values("Toplam_Ciro", ascending=False)
                 .head(10)
    )

    offline_result = (
        offline_df.groupby(STORE_COL)
                  .agg(
                      Toplam_Adet=(QTY_COL, "sum"),
                      Toplam_Ciro=(REVENUE_COL, "sum")
                  )
                  .reset_index()
                  .sort_values("Toplam_Ciro", ascending=False)
                  .head(10)
    )

    return online_result, offline_result


def get_store_online_offline_all_df(df: pd.DataFrame):
    """
    Online ve fiziksel mağazalar için TÜM mağaza özetlerini döner.
    Ciroya göre büyükten küçüğe sıralar, kırpma (head) yapmaz.
    """
    mask_online = df[STORE_COL].isin(ONLINE_STORES)
    online_df = df[mask_online]
    offline_df = df[~mask_online]

    online_result = (
        online_df.groupby(STORE_COL)
                 .agg(
                     Toplam_Adet=(QTY_COL, "sum"),
                     Toplam_Ciro=(REVENUE_COL, "sum")
                 )
                 .reset_index()
                 .sort_values("Toplam_Ciro", ascending=False)
    )

    offline_result = (
        offline_df.groupby(STORE_COL)
                  .agg(
                      Toplam_Adet=(QTY_COL, "sum"),
                      Toplam_Ciro=(REVENUE_COL, "sum")
                  )
                  .reset_index()
                  .sort_values("Toplam_Ciro", ascending=False)
    )

    return online_result, offline_result


def get_channels_df(df: pd.DataFrame) -> pd.DataFrame:
    valid = df[df[ORG_COL].notna()]
    online_df = valid[valid[ORG_COL] > 5000]
    offline_df = valid[valid[ORG_COL] <= 5000]

    rows = [
        {
            "Kanal": "Online",
            "Toplam_Adet": online_df[QTY_COL].sum(),
            "Toplam_Ciro": online_df[REVENUE_COL].sum()
        },
        {
            "Kanal": "Fiziksel",
            "Toplam_Adet": offline_df[QTY_COL].sum(),
            "Toplam_Ciro": offline_df[REVENUE_COL].sum()
        }
    ]
    return pd.DataFrame(rows)


def get_renewed_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    col = find_renewed_column(df)
    if col is None:
        return pd.DataFrame(columns=["Yenilenmis_Kolon", "Toplam_Adet", "Toplam_Ciro"])

    mask = df[col].astype(str).str.strip().str.upper() == "X"
    renewed_df = df[mask]

    return pd.DataFrame([{
        "Yenilenmis_Kolon": col,
        "Toplam_Adet": renewed_df[QTY_COL].sum(),
        "Toplam_Ciro": renewed_df[REVENUE_COL].sum()
    }])


def get_renewed_by_category_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Yenilenmiş (refurbished) ürünleri kategori bazında özetler.
    - Yenilenmiş kolonu X olan satırlar filtrelenir
    - Tercihen Kategori3, yoksa Kategori2 bazında adet ve ciro toplanır
    - Toplam ciroya göre azalan şekilde sıralanır
    """
    col = find_renewed_column(df)
    # Kategori kolonu olarak öncelik Kategori3'te
    if col is None:
        return pd.DataFrame()

    category_col = CATEGORY3_COL if CATEGORY3_COL in df.columns else CATEGORY_COL
    if category_col not in df.columns:
        return pd.DataFrame()

    mask = df[col].astype(str).str.strip().str.upper() == "X"
    renewed_df = df[mask]

    if renewed_df.empty:
        return pd.DataFrame()

    return (
        renewed_df.groupby(category_col)
                  .agg(
                      Toplam_Adet=(QTY_COL, "sum"),
                      Toplam_Ciro=(REVENUE_COL, "sum")
                  )
                  .reset_index()
                  .sort_values("Toplam_Ciro", ascending=False)
    )



def get_top_products_df(df: pd.DataFrame) -> pd.DataFrame:
    if PRODUCT_COL not in df.columns:
        return pd.DataFrame()

    return (
        df.groupby(PRODUCT_COL)
          .agg(
              Toplam_Adet=(QTY_COL, "sum"),
              Toplam_Ciro=(REVENUE_COL, "sum")
          )
          .reset_index()
          .sort_values("Toplam_Adet", ascending=False)
          .head(10)
    )


def get_top_products_top50_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ana datada en çok satılan ilk 50 ürünü döner.
    Adet bazında büyükten küçüğe sıralar.
    """
    if PRODUCT_COL not in df.columns:
        return pd.DataFrame()

    return (
        df.groupby(PRODUCT_COL)
          .agg(
              Toplam_Adet=(QTY_COL, "sum"),
              Toplam_Ciro=(REVENUE_COL, "sum")
          )
          .reset_index()
          .sort_values("Toplam_Adet", ascending=False)
          .head(50)
    )


def get_giftcard_products_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    required_cols = [GC_PRODUCT_COL, GC_QTY_COL, GC_INVOICE_COL, GC_DISC_COL]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        return pd.DataFrame()

    return (
        df.groupby(GC_PRODUCT_COL)
          .agg(
              Toplam_Adet=(GC_QTY_COL, "sum"),
              Toplam_Fatura_Tutari=(GC_INVOICE_COL, "sum"),
              Toplam_Indirim_Tutari=(GC_DISC_COL, "sum"),
          )
          .reset_index()
          .sort_values("Toplam_Adet", ascending=False)
    )


# === EKRANA YAZAN FONKSİYONLAR (ARTIK YUKARIDAKİ DF FONKSİYONLARINI KULLANIYOR) ===

def print_total(df: pd.DataFrame):
    result = get_total_df(df)
    row = result.iloc[0]

    print("\n1) GENEL TOPLAM (SATIŞ DATASI)")
    print(f"Toplam Adet : {row['Toplam_Adet']:,.0f}")
    print(f"Toplam Ciro : {row['Toplam_Ciro']:,.2f} TL")
    print("-" * 60)


def print_category(df: pd.DataFrame):
    result = get_category_df(df)
    print("\n2) KATEGORİ BAZLI TOPLAM (Kategori2)")
    print(result.to_string(index=False))
    print("-" * 60)


def print_brand(df: pd.DataFrame):
    result = get_brand_top10_df(df)
    print("\n3) MARKA BAZLI TOP 10")
    print(result.to_string(index=False))
    print("-" * 60)


def print_store(df: pd.DataFrame):
    online_result, offline_result = get_store_online_offline_df(df)

    print("\n4) MAĞAZA BAZLI TOP 10 - ONLINE")
    if not online_result.empty:
        print(online_result.to_string(index=False))
    else:
        print("Online mağaza bulunamadı.")
    print("-" * 60)

    print("\n4) MAĞAZA BAZLI TOP 10 - FİZİKSEL")
    if not offline_result.empty:
        print(offline_result.to_string(index=False))
    else:
        print("Fiziksel mağaza bulunamadı.")
    print("-" * 60)


def print_channels(df: pd.DataFrame):
    result = get_channels_df(df)

    print("\n5) KANAL BAZLI TOPLAM (SATIŞ DATASI)")
    for _, row in result.iterrows():
        print(f"\n{row['Kanal'].upper()} SATIŞLAR")
        print(f"Adet: {row['Toplam_Adet']:,.0f}")
        print(f"Ciro: {row['Toplam_Ciro']:,.2f} TL")
    print("-" * 60)


def print_renewed(df: pd.DataFrame):
    result = get_renewed_summary_df(df)

    print("\n6) YENİLENMİŞ ÜRÜNLER TOPLAMI")
    if result.empty:
        print("Yenilenmiş ürün sütunu bulunamadı veya veri yok.")
        print("-" * 60)
        return

    row = result.iloc[0]
    print(f"Kullanılan sütun adı             : {row['Yenilenmis_Kolon']}")
    print(f"Toplam Yenilenmiş Ürün Adedi     : {row['Toplam_Adet']:,.0f}")
    print(f"Toplam Yenilenmiş Ürün Cirosu    : {row['Toplam_Ciro']:,.2f} TL")
    print("-" * 60)


def print_top_products(df: pd.DataFrame):
    result = get_top_products_df(df)

    print("\n7) EN ÇOK SATILAN ÜRÜNLER - TOP 10")
    if result.empty:
        print("Top ürünler listesi oluşturulamadı (Uzun Tanım kolonu yok veya veri yok).")
        print("-" * 60)
        return

    print(result.to_string(index=False))
    print("-" * 60)


def print_giftcard_products(df: pd.DataFrame):
    """
    Gift card datasında hangi ürünlerin kaçar adet geldiğini,
    brüt fatura tutarlarını ve toplam indirim tutarını ürün bazında gösterir.
    """
    print("\n8) GIFT CARD ÜRÜN TOPLAMLARI")

    if df is None or df.empty:
        print("Gift card datası boş, bu rapor oluşturulamadı.")
        print("-" * 60)
        return

    required_cols = [GC_PRODUCT_COL, GC_QTY_COL, GC_INVOICE_COL, GC_DISC_COL]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print("Gift card datasında beklenen sütunlar eksik!")
        print("Eksik sütunlar :", missing)
        print("Mevcut sütunlar:", list(df.columns))
        print("-" * 60)
        return

    result = get_giftcard_products_df(df)
    if result.empty:
        print("Gift card datası boş, gösterilecek ürün bulunamadı.")
        print("-" * 60)
        return

    print(result.to_string(index=False))
    print("-" * 60)


# === ÜRÜN ARAMA ===

def query_product(df: pd.DataFrame, search: str):
    """
    Ana satış datasında 'Uzun Tanım' sütununda geçen metne göre
    ürünleri filtreler ve adet + ciro toplamını gösterir.
    Arama case-insensitive ve kısmi eşleşme ile yapılır.
    """
    print(f"\n🔍 Arama: '{search}'")

    if PRODUCT_COL not in df.columns:
        print(f"{PRODUCT_COL} sütunu bulunamadı, ürün bazlı arama yapılamıyor.")
        print("-" * 60)
        return

    # Arama filtresi (case-insensitive, kısmi eşleşme)
    mask = df[PRODUCT_COL].astype(str).str.contains(search, case=False, na=False)
    sub = df[mask]

    if sub.empty:
        print("Bu arama ile eşleşen ürün bulunamadı.")
        print("-" * 60)
        return

    # Her bir "Uzun Tanım" için toplam adet ve ciro
    result = (
        sub.groupby(PRODUCT_COL)
           .agg(
               Toplam_Adet=(QTY_COL, "sum"),
               Toplam_Ciro=(REVENUE_COL, "sum")
           )
           .reset_index()
           .sort_values("Toplam_Adet", ascending=False)
    )

    print(result.to_string(index=False))
    print("-" * 60)


def interactive_product_lookup(df: pd.DataFrame):
    """
    Konsolda kullanıcıdan ürün adı alarak tekrar tekrar arama yapmayı sağlar.
    Enter'a basarak çıkılabilir.
    """
    print("\n9) ÜRÜN BAZLI HIZLI SORGULAMA MODU")
    print("Belirli bir ürün için adet & ciro görmek istersen ürün adından bir parça yaz.")
    print("Çıkmak için hiçbir şey yazmadan Enter'a bas.\n")

    while True:
        try:
            search = input("→ Ürün adı/ifade gir (veya Enter ile çık): ").strip()
        except EOFError:
            print("\nInput alınamadı, ürün sorgulama modu atlandı.")
            print("-" * 60)
            break

        if not search:
            print("\n🔚 Ürün arama modu kapatıldı.")
            print("📦 Program sonlandırılıyor...\n")
            print("-" * 60)
            break

        query_product(df, search)


# === EXCEL RAPOR ÜRETİCİ ===

def export_to_excel(df: pd.DataFrame, df_gc: pd.DataFrame, bad_sales: int, bad_gift: int):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"statvision_report_{timestamp}.xlsx"

    total_df = get_total_df(df)
    category_df = get_category_df(df)
    brand_top10_df = get_brand_top10_df(df)        # terminal için
    brand_all_df = get_brand_all_df(df)            # Excel MarkaTotal için
    store_online_top10_df, store_offline_top10_df = get_store_online_offline_df(df)   # terminal için
    store_online_all_df, store_offline_all_df = get_store_online_offline_all_df(df)   # Excel için
    channels_df = get_channels_df(df)              # sheet yok ama Summary için kullanıyoruz
    renewed_summary_df = get_renewed_summary_df(df)
    renewed_by_cat_df = get_renewed_by_category_df(df)
    top_products_df = get_top_products_df(df)      # terminal için (Top10)
    top_products50_df = get_top_products_top50_df(df)  # Excel ProductTotal için
    giftcard_df = get_giftcard_products_df(df_gc)

    # Summary sheet için küçük bir özet tablo
    summary_rows = []

    if not total_df.empty:
        summary_rows.append({
            "Metrix": "Toplam Adet",
            "Deger": total_df.iloc[0]["Toplam_Adet"]
        })
        summary_rows.append({
            "Metrix": "Toplam Ciro",
            "Deger": total_df.iloc[0]["Toplam_Ciro"]
        })

    for _, row in channels_df.iterrows():
        summary_rows.append({
            "Metrix": f"{row['Kanal']} Adet",
            "Deger": row["Toplam_Adet"]
        })
        summary_rows.append({
            "Metrix": f"{row['Kanal']} Ciro",
            "Deger": row["Toplam_Ciro"]
        })

    if not renewed_summary_df.empty:
        r = renewed_summary_df.iloc[0]
        summary_rows.append({
            "Metrix": "Yenilenmiş Ürün Adedi",
            "Deger": r["Toplam_Adet"]
        })
        summary_rows.append({
            "Metrix": "Yenilenmiş Ürün Cirosu",
            "Deger": r["Toplam_Ciro"]
        })

    summary_rows.append({
        "Metrix": "Satış datası bozuk satır sayısı",
        "Deger": bad_sales
    })
    summary_rows.append({
        "Metrix": "Gift card datası bozuk satır sayısı",
        "Deger": bad_gift
    })

    summary_df = pd.DataFrame(summary_rows)

    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        workbook = writer.book

        # ==== ORTAK FORMATLAR ====
        title_format = workbook.add_format({
            "bold": True,
            "font_size": 14,
            "align": "left"
        })

        subtitle_format = workbook.add_format({
            "font_size": 10,
            "italic": True,
            "align": "left",
            "font_color": "#666666"
        })

        header_format = workbook.add_format({
            "bold": True,
            "bg_color": "#D9E1F2",
            "border": 1,
            "align": "center",
            "valign": "vcenter"
        })

        metric_text_format = workbook.add_format({
            "border": 1,
            "align": "left",
            "valign": "vcenter"
        })

        number_format_int = workbook.add_format({
            "border": 1,
            "align": "right",
            "valign": "vcenter",
            "num_format": "#,##0"
        })

        number_format_dec = workbook.add_format({
            "border": 1,
            "align": "right",
            "valign": "vcenter",
            "num_format": "#,##0.00"
        })

        # ================== SUMMARY SHEET ==================
        summary_ws = workbook.add_worksheet("Summary")
        writer.sheets["Summary"] = summary_ws

        summary_ws.merge_range("A1:B1", "STATVISION - Satış Özeti", title_format)
        summary_ws.merge_range(
            "A2:B2",
            f"Rapor Tarihi: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            subtitle_format
        )

        start_row = 3
        summary_ws.write(start_row, 0, "Metrix", header_format)
        summary_ws.write(start_row, 1, "Değer", header_format)

        for i, row in summary_df.iterrows():
            excel_row = start_row + 1 + i
            summary_ws.write(excel_row, 0, row["Metrix"], metric_text_format)

            if pd.isna(row["Deger"]):
                summary_ws.write(excel_row, 1, "", number_format_int)
            else:
                summary_ws.write(excel_row, 1, row["Deger"], number_format_int)

        summary_ws.set_column("A:A", 32)
        summary_ws.set_column("B:B", 18)
        summary_ws.freeze_panes(start_row + 1, 0)

        # ================== KATEGORI SHEET ==================
        category_ws = workbook.add_worksheet("Kategori")
        writer.sheets["Kategori"] = category_ws

        kategori_sayisi = len(category_df)

        category_ws.merge_range("A1:C1", "Kategori Bazlı Satış Özeti", title_format)
        category_ws.merge_range(
            "A2:C2",
            f"Bu dönemde toplam {kategori_sayisi} farklı kategoriden data gelmiştir.",
            subtitle_format
        )

        start_row_cat = 3

        for col_idx, col_name in enumerate(category_df.columns):
            category_ws.write(start_row_cat, col_idx, col_name, header_format)

        for i, row in category_df.iterrows():
            excel_row = start_row_cat + 1 + i
            category_ws.write(excel_row, 0, row[category_df.columns[0]], metric_text_format)
            if "Toplam_Adet" in category_df.columns:
                category_ws.write(excel_row, 1, row["Toplam_Adet"], number_format_int)
            if "Toplam_Ciro" in category_df.columns:
                category_ws.write(excel_row, 2, row["Toplam_Ciro"], number_format_dec)

        total_row_cat = start_row_cat + 1 + len(category_df) + 1
        total_label_fmt = workbook.add_format({"bold": True, "align": "left"})
        category_ws.write(total_row_cat, 0, "Kategori Sayısı", total_label_fmt)
        category_ws.write(total_row_cat, 1, kategori_sayisi, number_format_int)

        category_ws.set_column("A:A", 28)
        category_ws.set_column("B:B", 18)
        category_ws.set_column("C:C", 20)
        category_ws.freeze_panes(start_row_cat + 1, 1)

        # ================== MARKA SHEET (MarkaTotal, TÜM MARKALAR) ==================
        brand_ws = workbook.add_worksheet("MarkaTotal")
        writer.sheets["MarkaTotal"] = brand_ws

        marka_sayisi = len(brand_all_df)

        brand_ws.merge_range("A1:C1", "Marka Bazlı Satış Özeti", title_format)
        brand_ws.merge_range(
            "A2:C2",
            f"Bu dönemde toplam {marka_sayisi} farklı markadan satış gerçekleşmiştir.",
            subtitle_format
        )

        start_row_brand = 3

        for col_idx, col_name in enumerate(brand_all_df.columns):
            brand_ws.write(start_row_brand, col_idx, col_name, header_format)

        for i, row in brand_all_df.iterrows():
            excel_row = start_row_brand + 1 + i
            brand_ws.write(excel_row, 0, row[brand_all_df.columns[0]], metric_text_format)
            if "Toplam_Adet" in brand_all_df.columns:
                brand_ws.write(excel_row, 1, row["Toplam_Adet"], number_format_int)
            if "Toplam_Ciro" in brand_all_df.columns:
                brand_ws.write(excel_row, 2, row["Toplam_Ciro"], number_format_dec)

        total_row_brand = start_row_brand + 1 + len(brand_all_df) + 1
        brand_ws.write(total_row_brand, 0, "Marka Sayısı", total_label_fmt)
        brand_ws.write(total_row_brand, 1, marka_sayisi, number_format_int)

        brand_ws.set_column("A:A", 28)
        brand_ws.set_column("B:B", 18)
        brand_ws.set_column("C:C", 20)
        brand_ws.freeze_panes(start_row_brand + 1, 1)

        # ================== ONLINE MAĞAZA SHEET (MagazaOnlineTotal) ==================
        online_ws = workbook.add_worksheet("MagazaOnlineTotal")
        writer.sheets["MagazaOnlineTotal"] = online_ws

        online_store_count = len(store_online_all_df)

        online_ws.merge_range("A1:C1", "Online Mağaza Bazlı Satış Özeti", title_format)
        online_ws.merge_range(
            "A2:C2",
            f"Bu dönemde toplam {online_store_count} farklı online mağazadan satış gerçekleşmiştir.",
            subtitle_format
        )

        start_row_online = 3

        for col_idx, col_name in enumerate(store_online_all_df.columns):
            online_ws.write(start_row_online, col_idx, col_name, header_format)

        for i, row in store_online_all_df.iterrows():
            excel_row = start_row_online + 1 + i
            online_ws.write(excel_row, 0, row[store_online_all_df.columns[0]], metric_text_format)
            if "Toplam_Adet" in store_online_all_df.columns:
                online_ws.write(excel_row, 1, row["Toplam_Adet"], number_format_int)
            if "Toplam_Ciro" in store_online_all_df.columns:
                online_ws.write(excel_row, 2, row["Toplam_Ciro"], number_format_dec)

        total_row_online = start_row_online + 1 + len(store_online_all_df) + 1
        online_ws.write(total_row_online, 0, "Online Mağaza Sayısı", total_label_fmt)
        online_ws.write(total_row_online, 1, online_store_count, number_format_int)

        online_ws.set_column("A:A", 28)
        online_ws.set_column("B:B", 18)
        online_ws.set_column("C:C", 20)
        online_ws.freeze_panes(start_row_online + 1, 1)

        # ================== FİZİKSEL MAĞAZA SHEET (MagazaFizikselTotal) ==================
        offline_ws = workbook.add_worksheet("MagazaFizikselTotal")
        writer.sheets["MagazaFizikselTotal"] = offline_ws

        offline_store_count = len(store_offline_all_df)

        offline_ws.merge_range("A1:C1", "Fiziksel Mağaza Bazlı Satış Özeti", title_format)
        offline_ws.merge_range(
            "A2:C2",
            f"Bu dönemde toplam {offline_store_count} farklı fiziksel mağazadan satış gerçekleşmiştir.",
            subtitle_format
        )

        start_row_offline = 3

        for col_idx, col_name in enumerate(store_offline_all_df.columns):
            offline_ws.write(start_row_offline, col_idx, col_name, header_format)

        for i, row in store_offline_all_df.iterrows():
            excel_row = start_row_offline + 1 + i
            offline_ws.write(excel_row, 0, row[store_offline_all_df.columns[0]], metric_text_format)
            if "Toplam_Adet" in store_offline_all_df.columns:
                offline_ws.write(excel_row, 1, row["Toplam_Adet"], number_format_int)
            if "Toplam_Ciro" in store_offline_all_df.columns:
                offline_ws.write(excel_row, 2, row["Toplam_Ciro"], number_format_dec)

        total_row_offline = start_row_offline + 1 + len(store_offline_all_df) + 1
        offline_ws.write(total_row_offline, 0, "Fiziksel Mağaza Sayısı", total_label_fmt)
        offline_ws.write(total_row_offline, 1, offline_store_count, number_format_int)

        offline_ws.set_column("A:A", 28)
        offline_ws.set_column("B:B", 18)
        offline_ws.set_column("C:C", 20)
        offline_ws.freeze_panes(start_row_offline + 1, 1)

        # ================== REFURBISHED SHEET (RefurbishedTotal, Kategori3 bazlı) ==================
        ref_ws = workbook.add_worksheet("RefurbishedTotal")
        writer.sheets["RefurbishedTotal"] = ref_ws

        ref_ws.merge_range("A1:C1", "Refurbished (Yenilenmiş) Ürün Özeti", title_format)

        if renewed_summary_df.empty:
            ref_ws.merge_range(
                "A2:C2",
                "Bu dönemde yenilenmiş (refurbished) ürün satışı bulunmamaktadır.",
                subtitle_format
            )
        else:
            r = renewed_summary_df.iloc[0]
            toplam_adet = r["Toplam_Adet"]
            toplam_ciro = r["Toplam_Ciro"]

            ref_ws.merge_range(
                "A2:C2",
                f"Bu dönemde toplam {toplam_adet:,.0f} adet ve {toplam_ciro:,.2f} TL refurbished ürün satılmıştır.",
                subtitle_format
            )

            if not renewed_by_cat_df.empty:
                start_row_ref = 3

                for col_idx, col_name in enumerate(renewed_by_cat_df.columns):
                    ref_ws.write(start_row_ref, col_idx, col_name, header_format)

                for i, row in renewed_by_cat_df.iterrows():
                    excel_row = start_row_ref + 1 + i
                    ref_ws.write(excel_row, 0, row[renewed_by_cat_df.columns[0]], metric_text_format)
                    if "Toplam_Adet" in renewed_by_cat_df.columns:
                        ref_ws.write(excel_row, 1, row["Toplam_Adet"], number_format_int)
                    if "Toplam_Ciro" in renewed_by_cat_df.columns:
                        ref_ws.write(excel_row, 2, row["Toplam_Ciro"], number_format_dec)

                ref_ws.set_column("A:A", 30)
                ref_ws.set_column("B:B", 18)
                ref_ws.set_column("C:C", 20)
                ref_ws.freeze_panes(start_row_ref + 1, 1)

        # ================== PRODUCT SHEET (ProductTotal, ilk 50 ürün) ==================
        prod_ws = workbook.add_worksheet("ProductTotal")
        writer.sheets["ProductTotal"] = prod_ws

        prod_ws.merge_range("A1:C1", "En Çok Satılan Ürünler - İlk 50", title_format)

        if top_products50_df.empty:
            # Veri yoksa bilgi notu yaz
            prod_ws.merge_range(
                "A2:C2",
                "Bu dönemde ürün satış verisi bulunamadı.",
                subtitle_format
            )
            # Yine de başlık satırını yazmak istersen:
            start_row_prod = 3
            for col_idx, col_name in enumerate(["Uzun Tanım", "Toplam_Adet", "Toplam_Ciro"]):
                prod_ws.write(start_row_prod, col_idx, col_name, header_format)

        else:
            # Veri varsa açıklama
            prod_ws.merge_range(
                "A2:C2",
                "Bu sayfada adet bazında en çok satılan ilk 50 ürün listelenmiştir.",
                subtitle_format
            )

            start_row_prod = 3

            # Başlık satırı
            for col_idx, col_name in enumerate(top_products50_df.columns):
                prod_ws.write(start_row_prod, col_idx, col_name, header_format)

            # Ürün satırları (index yerine enumerate kullanıyoruz)
            for row_idx, (_, row) in enumerate(top_products50_df.iterrows()):
                excel_row = start_row_prod + 1 + row_idx

                # 0: ürün adı (Uzun Tanım)
                prod_ws.write(excel_row, 0, row[top_products50_df.columns[0]], metric_text_format)

                # 1: adet
                if "Toplam_Adet" in top_products50_df.columns:
                    prod_ws.write(excel_row, 1, row["Toplam_Adet"], number_format_int)

                # 2: ciro
                if "Toplam_Ciro" in top_products50_df.columns:
                    prod_ws.write(excel_row, 2, row["Toplam_Ciro"], number_format_dec)

        # Genel görünüm ayarları
        prod_ws.set_column("A:A", 60)  # ürün ismi uzun olabilir
        prod_ws.set_column("B:B", 18)
        prod_ws.set_column("C:C", 20)
        prod_ws.freeze_panes(start_row_prod + 1, 1)


                # ================== GIFT CARD SHEET (GiftCardTotal) ==================
        gift_ws = workbook.add_worksheet("GiftCardTotal")
        writer.sheets["GiftCardTotal"] = gift_ws

        gift_ws.merge_range("A1:D1", "Gift Card Ürün Özeti", title_format)

        if giftcard_df.empty:
            # Veri yoksa sadece bilgi notu + başlıkları yaz
            gift_ws.merge_range(
                "A2:D2",
                "Bu dönemde gift card datası bulunmamaktadır.",
                subtitle_format
            )

            start_row_gc = 3
            headers = ["Ürün", "Toplam_Adet", "Toplam_Fatura_Tutari", "Toplam_Indirim_Tutari"]
            for col_idx, col_name in enumerate(headers):
                gift_ws.write(start_row_gc, col_idx, col_name, header_format)

        else:
            toplam_gift_urun = len(giftcard_df)

            gift_ws.merge_range(
                "A2:D2",
                f"Bu sayfada gift card datasındaki {toplam_gift_urun} ürünün adet ve tutar detayları listelenmiştir.",
                subtitle_format
            )

            start_row_gc = 3

            # Başlık satırı (fonksiyonun ürettiği kolon adlarıyla)
            for col_idx, col_name in enumerate(giftcard_df.columns):
                gift_ws.write(start_row_gc, col_idx, col_name, header_format)

            # Satırlar
            for row_idx, (_, row) in enumerate(giftcard_df.iterrows()):
                excel_row = start_row_gc + 1 + row_idx

                # 0: Ürün adı (GC_PRODUCT_COL)
                gift_ws.write(excel_row, 0, row[giftcard_df.columns[0]], metric_text_format)

                # 1: Toplam_Adet
                if "Toplam_Adet" in giftcard_df.columns:
                    gift_ws.write(excel_row, 1, row["Toplam_Adet"], number_format_int)

                # 2: Toplam_Fatura_Tutari
                if "Toplam_Fatura_Tutari" in giftcard_df.columns:
                    gift_ws.write(excel_row, 2, row["Toplam_Fatura_Tutari"], number_format_dec)

                # 3: Toplam_Indirim_Tutari
                if "Toplam_Indirim_Tutari" in giftcard_df.columns:
                    gift_ws.write(excel_row, 3, row["Toplam_Indirim_Tutari"], number_format_dec)

        # Görünüm ayarları
        gift_ws.set_column("A:A", 40)  # ürün adı
        gift_ws.set_column("B:B", 18)  # adet
        gift_ws.set_column("C:D", 22)  # tutarlar
        gift_ws.freeze_panes(start_row_gc + 1, 1)


    print(f"\n📊 Excel raporu oluşturuldu: {output_file}")
    print("-" * 60)


# === MAIN ===

def main():
    print_banner()

    # Bozuk satır sayıları
    print("Bozuk satırlar analiz ediliyor...")
    bad_sales = count_bad_lines(FILE_PATH)
    bad_gift = count_bad_lines(GIFTCARD_FILE_PATH)

    # Ana satış datası
    df = load_data(FILE_PATH)

    print_total(df)
    print_category(df)
    print_brand(df)
    print_store(df)
    print_channels(df)
    print_renewed(df)
    print_top_products(df)

    # Gift card datası
    df_gc = load_giftcard_data(GIFTCARD_FILE_PATH)
    print_giftcard_products(df_gc)

    # Excel raporu
    export_to_excel(df, df_gc, bad_sales, bad_gift)

    print(f"\n⚠️ Satış datasında bozuk satır sayısı    : {bad_sales:,}")
    print(f"⚠️ Gift card datasında bozuk satır sayısı: {bad_gift:,}")
    print("-" * 60)

    # Ürün bazlı interaktif sorgulama
    interactive_product_lookup(df)

    print_goodbye()


if __name__ == "__main__":
    main()
