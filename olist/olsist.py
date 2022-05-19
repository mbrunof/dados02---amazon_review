import pandas as pd
import numpy as np

print("Carregando os dados")

df_olist_marketing=pd.read_csv("data/olist_marketing_qualified_leads_dataset.csv")
df_olist_closed=pd.read_csv("data/olist_closed_deals_dataset.csv")

print("Criando a coluna com quais MQLs fecharam o acordo")

df_olist_closed["fechou_acordo"]="sim"
df_olist_merge=df_olist_marketing.merge(df_olist_closed, how="left", on=["mql_id"])
df_olist_merge["fechou_acordo"] = df_olist_merge["fechou_acordo"].fillna("nao")

print("-------------------------------------")

def taxa_de_conversao(df_olist_merge:pd.DataFrame) -> str:
    return print("Qual foi a taxa de conversão total?", (df_olist_merge[df_olist_merge["fechou_acordo"]=="sim"].shape[0]/df_olist_merge.shape[0])*100, "%")

taxa_de_conversao(df_olist_merge)

print("-------------------------------------")

def taxa_de_conversao_por_origem(df_olist_merge: pd.DataFrame) -> pd.DataFrame:
    df_olist_merge_taxa_conv_ori = pd.DataFrame(df_olist_merge.origin.value_counts()).reset_index()
    df_olist_merge_taxa_conv_ori.columns = ['origin', 'total_de_mql_por_origem']

    df_olist_merge_conve_sim = df_olist_merge[df_olist_merge["fechou_acordo"] == "sim"]
    df_olist_merge_conve_sim = pd.DataFrame(
        df_olist_merge_conve_sim.groupby("origin")["fechou_acordo"].agg("count")).reset_index()
    df_olist_merge_conve_sim.columns = ["origin", "mql_fechou_acordo_por_origem"]

    df_olist_merge_taxa_conv_ori = df_olist_merge_taxa_conv_ori.merge(df_olist_merge_conve_sim, on=["origin"],
                                                                      how="inner")

    df_olist_merge_taxa_conv_ori["taxa_de_conversao_por_origem"] = df_olist_merge_taxa_conv_ori[
                                                                       "mql_fechou_acordo_por_origem"] / \
                                                                   df_olist_merge_taxa_conv_ori[
                                                                       "total_de_mql_por_origem"]
    df_olist_merge_taxa_conv_ori["taxa_de_conversao_por_origem"] = df_olist_merge_taxa_conv_ori[
                                                                       "taxa_de_conversao_por_origem"] * 100

    print("Qual foi a taxa de conversão de cada origem?\n",
          df_olist_merge_taxa_conv_ori[["origin", "taxa_de_conversao_por_origem"]])

    return df_olist_merge_taxa_conv_ori[["origin", "taxa_de_conversao_por_origem"]]


df_taxa_de_conversao_por_origem = taxa_de_conversao_por_origem(df_olist_merge)

print("-------------------------------------")

def taxa_de_conversao_por_pagina(df_olist_merge: pd.DataFrame) -> pd.DataFrame:
    df_olist_merge_taxa_conv_pag = pd.DataFrame(df_olist_merge.landing_page_id.value_counts()).reset_index()
    df_olist_merge_taxa_conv_pag.columns = ['landing_page_id', 'total_de_mql_por_pagina']

    df_olist_merge_conve_sim = df_olist_merge[df_olist_merge["fechou_acordo"] == "sim"]
    df_olist_merge_conve_sim = pd.DataFrame(
        df_olist_merge_conve_sim.groupby("landing_page_id")["fechou_acordo"].agg("count")).reset_index()
    df_olist_merge_conve_sim.columns = ["landing_page_id", "mql_fechou_acordo_por_pagina"]

    df_olist_merge_taxa_conv_pag = df_olist_merge_taxa_conv_pag.merge(df_olist_merge_conve_sim, on=["landing_page_id"],
                                                                      how="inner")

    df_olist_merge_taxa_conv_pag["taxa_de_conversao_por_pagina"] = df_olist_merge_taxa_conv_pag[
                                                                       "mql_fechou_acordo_por_pagina"] / \
                                                                   df_olist_merge_taxa_conv_pag[
                                                                       "total_de_mql_por_pagina"]
    df_olist_merge_taxa_conv_pag["taxa_de_conversao_por_pagina"] = df_olist_merge_taxa_conv_pag[
                                                                       "taxa_de_conversao_por_pagina"] * 100

    print("Qual foi a taxa de conversão de cada página inicial?\n",
          df_olist_merge_taxa_conv_pag[["landing_page_id", "taxa_de_conversao_por_pagina"]])

    return df_olist_merge_taxa_conv_pag[["landing_page_id", "taxa_de_conversao_por_pagina"]]


df_taxa_de_conversao_por_pagina = taxa_de_conversao_por_pagina(df_olist_merge)

print("-------------------------------------")

def receita_media_por_rs(df_olist_merge: pd.DataFrame) -> pd.DataFrame:
    df_olist_group = pd.DataFrame(df_olist_merge[df_olist_merge["fechou_acordo"] == "sim"].groupby("sdr_id")[
                                      "declared_monthly_revenue"].mean()).reset_index()
    df_olist_group.columns = ["sdr_id", "declared_monthly_revenue_mean"]
    df_olist_group["declared_monthly_revenue_mean"] = df_olist_group["declared_monthly_revenue_mean"].round(1)

    print("Para cada SR, qual a receita média declarada dos leads?\n", df_olist_group)

    return df_olist_group

df_receita_media_por_rs = receita_media_por_rs(df_olist_merge)

print("-------------------------------------")


def business_type_convertido_por_rs(df_olist_merge: pd.DataFrame) -> pd.DataFrame:
    df_olist_group = pd.DataFrame(
        df_olist_merge[df_olist_merge["fechou_acordo"] == "sim"].groupby(["sdr_id", "business_type"])[
            "fechou_acordo"].count()).reset_index()
    df_olist_group.columns = ["sdr_id", "business_type", "conversao"]

    print("Para cada SR, quantos de cada business_type a pessoa converteu?\n", df_olist_group)

    return df_olist_group


df_business_type_convertido_por_rs = business_type_convertido_por_rs(df_olist_merge)

print("-------------------------------------")


def lead_behaviour_profile_convertido_por_rs(df_olist_merge: pd.DataFrame) -> pd.DataFrame:

    df_olist_group = df_olist_merge[df_olist_merge["fechou_acordo"] == "sim"]
    df_olist_group = df_olist_group[["sdr_id", "lead_behaviour_profile", "fechou_acordo"]]

    df_olist_group["lead_behaviour_profile"] = df_olist_group["lead_behaviour_profile"].str.split(", ")
    df_olist_group = df_olist_group.explode("lead_behaviour_profile")

    df_olist_group = pd.DataFrame(
        df_olist_group.groupby(["sdr_id", "lead_behaviour_profile"])["fechou_acordo"].count()).reset_index()

    print("Para cada SR, quantos de cada lead_behaviour_profile a pessoa converteu?\n", df_olist_group)

    return df_olist_group

df_lead_behaviour_profile_convertido_por_rs = lead_behaviour_profile_convertido_por_rs(df_olist_merge)

print("-------------------------------------")


def lead_type_convertido_por_rs(df_olist_merge: pd.DataFrame) -> pd.DataFrame:
    df_olist_group = pd.DataFrame(
        df_olist_merge[df_olist_merge["fechou_acordo"] == "sim"].groupby(["sdr_id", "lead_type"])[
            "fechou_acordo"].count()).reset_index()
    df_olist_group.columns = ["sdr_id", "lead_type", "conversao"]

    print("Para cada SR, quantos de cada lead_type a pessoa converteu?\n", df_olist_group)

    return df_olist_group


df_lead_type_convertido_por_rs = lead_type_convertido_por_rs(df_olist_merge)

print("-------------------------------------")
