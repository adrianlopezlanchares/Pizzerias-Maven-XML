# Pizzerias Maven tiene unos dataset de las pizzas que tienen en el menu, 
# tamaño, pedidos, etc. Como objetivo le gustaria poder saber que stock de 
# ingredientes deberian comprar a la semana, para optimizar el stock de 
# ingredientes y las compras de estos. 

# Importamos las librerias necesarias
import pandas as pd
import lxml.etree as ET
import re

def contarIngredientes(df, df_ingredientes):
    for i in range(len(df)):
        print("                                                 ", end="\r")
        print(f"\t\tProcesando fila {i+1}/{len(df)}", end="\r")
        ingredientes = df.loc[i, 'ingredients']
        ingredientes = ingredientes.split(",")
        for ingrediente in ingredientes:
            ingrediente = ingrediente.strip()
            if ingrediente in df_ingredientes['ingredient_name'].values:
                df_ingredientes.loc[df_ingredientes['ingredient_name'] == ingrediente, 'cantidad_semanal_necesaria'] += 1
            else:
                data = {'ingredient_name': ingrediente, 'cantidad_semanal_necesaria': 1}
                df_temp = pd.DataFrame.from_dict([data])
                df_ingredientes = pd.concat([df_ingredientes, df_temp])
    return df_ingredientes

def extraer(nombre_archivo):
    df = pd.read_csv(nombre_archivo, encoding = "ISO-8859-1")
    return df

def transformar(lista_df):
    df_pizzas = lista_df[0]         # Dataframe de pizzas, tamaño y precio
    df_pizza_types = lista_df[1]    # Dataframe de nombre y ingredientes de las pizzas
    df_orders = lista_df[2]         # Dataframe de pedidos y cuandos se hicieron
    df_order_details = lista_df[3]  # Dataframe de que pizzas pidio cada pedido

    # Creamos un nuevo dataframe con todos los datos
    df = pd.merge(df_orders, df_order_details, on='order_id')

    df = pd.merge(df, df_pizzas, on='pizza_id')

    df = pd.merge(df, df_pizza_types, on='pizza_type_id')
    df.sort_values(by=['order_details_id'], inplace=True)


    # Creamos un nuevo dataframe con los ingredientes y la cantidad de cada uno por semana
    df_ingredientes = pd.DataFrame(columns=['ingredient_name', 'cantidad_semanal_necesaria'])
    df.reset_index(inplace=True)

    # Calculamos la cantidad de ingredientes
    df_ingredientes = contarIngredientes(df, df_ingredientes)
    
    # Como el año tiene 52 semanas, la cantidad semanal necesaria sera aproximadamente (cantidad total total / 52)
    df_ingredientes['cantidad_semanal_necesaria']= df_ingredientes['cantidad_semanal_necesaria'] // 52

    return df_ingredientes, df

def escribirXML(df, df_ingredientes):
    # Escribimos el analisis de datos en un archivo XML
    root = ET.Element("columnas")
    tree = ET.ElementTree(root)
    for columna in df.columns:
        subelement = ET.SubElement(root, columna)
        subelement.text = str(df[columna].dtype)
    
    dict_ingredientes = df_ingredientes.set_index('ingredient_name').T.to_dict('list')
    recomendacion = ET.SubElement(root, "recomendacion")
    for ingrediente in dict_ingredientes:
        ingre_diente = ingrediente.replace(" ", "_")
        ingre_diente = re.sub('[^A-Za-z]+', '', ingre_diente)
        subelement = ET.SubElement(recomendacion, ingre_diente, attrib={"cantidad": str(dict_ingredientes[ingrediente][0])})
    
    with open("analisis.xml", "wb") as f:
        tree.write(f, pretty_print=True, xml_declaration=True, encoding="utf-8")

def etl():

    print(f"\t--> Extrayendo datos...")
    # Primero, extraemos los datos
    df_pizzas = extraer('pizzas.csv')
    df_pizza_types = extraer('pizza_types.csv')
    df_orders = extraer('orders.csv')
    df_order_details = extraer('order_details.csv')

    lista_df = [df_pizzas, df_pizza_types, df_orders, df_order_details]
    print(f"\t    Extraccion terminada.")

    # Segundo, transformamos los datos
    print(f"\t--> Transformando datos...")
    df, df_total = transformar(lista_df) 
    print()
    print(f"\t    Transformacion terminada.")
    
    # Tercero, cargamos los datos
    return df, df_total



def main():

    print("\n--> Empezando programa. Procesamos los datos con la ETL...")

    # Procesamos los datos mediante una ETL
    try:
        df_recomendaciones, df_total = etl()
    except:
        print("\nError al leer los datos. Falta algun archivo")
        print("Archivos necesarios:\n\t- practica1-pizzas.csv\n\t- practica1-pizza_types.csv\n\t- practica1-orders.csv\n\t- practica1-order_details.csv")
        return

    df_recomendaciones.to_csv("ingredientes_semanales.csv")
    escribirXML(df_total, df_recomendaciones)
    print("\n--> Datos procesados. Los datos se han guardado en 'ingredientes_semanales.csv'")
    print("--> Datos analizados. El analisis se ha guardado en 'analisis.xml'")

    return

if __name__ == '__main__':
    main()