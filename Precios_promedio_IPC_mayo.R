library("ini")
library("odbc")
library("openxlsx")
library("dplyr")

anio <- 2024
mes <- 5

# Solo se toma la presentación más popular, i.e. la cantidad que más aparece.
# Esta función toma la presentación más popular en cada departamento.
popular <- function(boletas) {
  
  # Las variedades a las que se les realiza un estudio de precio por gramo no
  # tendrán una presentación común. Por lo que separamos las boletas acorde.

  precio_por_gramo <- c(
    "1113011",  "1113021",  "1113022",  "1113041",  "1125032",  "1125034",
    "1145012",  "1145042",  "1143022",  "1161011",  "1161021",  "1161031",
    "1161041",  "1161051",  "1161061",  "1162011",  "1162012",  "1162021",
    "1165021",  "1165031",  "1171011",  "1171021",  "1171031",  "1171041",
    "1171051",  "1171061",  "1171062",  "1171071",  "1171081",  "1171091",
    "1172011",  "1172021",  "1172032",  "1172041",  "1172051",  "1174011",
    "1174021",  "1174041",  "1174051",  "4541011",  "1230011",  "1230012"
  )
  
  common <- boletas[!boletas$codigo_articulo %in% precio_por_gramo, ]
  uncommon <- boletas[boletas$codigo_articulo %in% precio_por_gramo, ]
  
  # Contamos cuantas veces aparece cada presentación
  counts <- boletas %>%
    group_by(codigo_articulo, Departamento, cantidad_actual) %>%
    summarize(count = n(), .groups = 'drop')
  
  # Determinamos la ocurrencia más alta en el conteo de cada grupo.
  max_counts <- counts %>%
    group_by(codigo_articulo, Departamento) %>%
    summarize(max_count = max(count), .groups = 'drop')
  
  # Tomamos sólo los conteos máximos. Puede haber varios máximos si más de una
  # presentación tiene la máxima ocurrencia.
  most_common <- counts %>%
    inner_join(max_counts, by = c("codigo_articulo", "Departamento")) %>%
    filter(count == max_count) %>%
    select(-count, -max_count)
  
  # Tomamos los articulos cuya presentación coincide con el conteo máximo en
  # cada departamento.
  common <- common %>%
    inner_join(
      most_common,
      by = c("codigo_articulo", "Departamento", "cantidad_actual")
    )
  
  # Volvemos a unir las boletas y las regresamos
  return(rbind(common, uncommon))
}

# Función para calcular el precio promedio
geo_mean <- function(x) {
  exp(mean(log(x[x > 0]), na.rm = TRUE))
}

# En lugar de reagrupar la base por region, variedad o producto, se puede usar
# el resultado de una agrupación anterior con una media geométrica ponderada.
w_geo_mean <- function(x, w) {
  exp(sum(w * log(x[x > 0]), na.rm = TRUE) / sum(w, na.rm = TRUE))
}

# Leer el archivo de configuración
config <- ini::read.ini("config.ini")

# Obtener las credenciales
server <- config$database$server
user <- config$database$user
password <- config$database$password
database <- config$database$database

# Crear la cadena de conexión
conn_str <- paste0(
  "Driver={ODBC Driver 17 for SQL Server};",
  "Server=", server, ";",
  "Database=", database, ";",
  "Uid=", user, ";",
  "Pwd=", password, ";"
)

# Establecer la conexión
con <- dbConnect(odbc::odbc(), .connection_string = conn_str)

bol_may <- dbGetQuery(
  con,
  paste(
    "EXEC [dbo].[sp_get_calculos_precios_recolectados_mes]",
    anio, ",",
    mes
  )
)

# Calculamos el precio base
bol_may$precio_base <- with(
  bol_may,
  precio_actual * cantidad_base / cantidad_actual
)

# Para fin de mes no debería haber boletas con precio base inválido.
# Pero por si acaso.
bol_may <- bol_may[
  !is.na(bol_may$precio_base) &
  bol_may$precio_base != 0,
]

base_may <- popular(bol_may)

# Guardamos las boletas que se utilizaron
write.xlsx(base_may, "Boletas_IPC_mayo.xlsx")

dep_may <- base_may %>%
  group_by(codigo_articulo, Departamento) %>%
  summarize(
    pgeo = geo_mean(precio_base),
    n = n(),
    # Estas columnas son únicas en cada grupo. Basta con tomar el primer valor
    # para conservarlas.
    producto_nombre = first(producto_nombre),
    cant_base = first(cantidad_base),
    articulo = first(articulo),
    region = first(region),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  ) %>% 
  # El Stored Process no incluye una columna para el código del producto pero
  # puede crearse retirando el último caracter en el código de la variedad.
  mutate(cod_prod = substr(codigo_articulo, 1, nchar(codigo_articulo) - 1)) %>%
  # Con una excepción, hay que remover dos dígitos en el case de "10400010".
  mutate(cod_prod = ifelse(cod_prod == "10400011", "1040001", cod_prod))

reg_may <- dep_may %>%
  group_by(codigo_articulo, region) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

var_may <- reg_may %>%
  group_by(codigo_articulo) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

prod_may <- var_may %>%
  group_by(cod_prod) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

# Usaremos mayo como base para determinar precios anómalos en meses anteriores.

mes <- 4

bol_abr <- dbGetQuery(
  con,
  paste(
    "EXEC [dbo].[sp_get_calculos_precios_recolectados_mes]",
    anio, ",",
    mes
  )
)

bol_abr$precio_base <- with(
  bol_abr,
  precio_actual * cantidad_base / cantidad_actual
)

bol_abr <- bol_abr[
  !is.na(bol_abr$precio_base) &
  bol_abr$precio_base != 0,
]

# Usamos sólo las boletas que no difieran en más de 25% con el precio promedio
# de su respectivo departamento en el mes siguiente.
bol_abr_sin_out <- bol_abr %>%
  left_join(
    dep_may %>% select(codigo_articulo, Departamento, pgeo),
    by = c("codigo_articulo", "Departamento")
    ) %>%
  # Si no hay precio el mes siguiente copiamos el precio base para no eliminar
  # esas boletas
  mutate(pgeo = coalesce(pgeo, precio_base)) %>%
  filter(precio_base <= pgeo * 1.25) %>%
  filter(precio_base >= pgeo / 1.25)

# De estas elegimos las presentaciones más populares
base_abr <- popular(bol_abr_sin_out)

# Guardamos las boletas que se utilizarán
write.xlsx(base_abr, "Boletas_IPC_abril.xlsx")

dep_abr <- base_abr %>%
  group_by(codigo_articulo, Departamento) %>%
  summarize(
    pgeo = geo_mean(precio_base),
    n = n(),
    producto_nombre = first(producto_nombre),
    cant_base = first(cantidad_base),
    articulo = first(articulo),
    region = first(region),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  ) %>% 
  mutate(cod_prod = substr(codigo_articulo, 1, nchar(codigo_articulo) - 1)) %>%
  mutate(cod_prod = ifelse(cod_prod == "10400011", "1040001", cod_prod))

# Vamos a incluir los precios de mayo retroactivamente en los precios de abril.

dep_abr_adj <- dep_abr %>%
  select(codigo_articulo, region, Departamento, n, pgeo) %>%
  full_join(dep_may %>% select(-n) %>% rename_at('pgeo', ~'pgeo_may'),
             by = c("codigo_articulo", "region", "Departamento")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_may))

dep_abr_adj$anio <- anio
dep_abr_adj$mes <- mes
dep_abr_adj$pgeo_may <- NULL

# Los precios imputados hacia atras no entrarán en el cálculo por region.
# Por lo que usaremos los precios de abril sin ajustar.
reg_abr <- dep_abr %>%
  group_by(codigo_articulo, region) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

# Un ajuste similar a los promedio por departamento
reg_abr_adj <- reg_abr %>%
  select(codigo_articulo, region, n, pgeo) %>%
  full_join(reg_may %>% select(-n) %>% rename_at('pgeo', ~'pgeo_may'),
             by = c("codigo_articulo", "region")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_may))

reg_abr_adj$anio <- anio
reg_abr_adj$mes <- mes
reg_abr_adj$pgeo_may <- NULL

# Sucede lo mismo con los precios por variedad

var_abr <- reg_abr %>%
  group_by(codigo_articulo) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

var_abr_adj <- var_abr %>%
  select(codigo_articulo, n, pgeo) %>%
  full_join(var_may %>% select(-n) %>% rename_at('pgeo', ~'pgeo_may'),
             by = c("codigo_articulo")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_may))

var_abr_adj$anio <- anio
var_abr_adj$mes <- mes
var_abr_adj$pgeo_may <- NULL

# Y los precios por producto

prod_abr <- var_abr %>%
  group_by(cod_prod) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

prod_abr_adj <- prod_abr %>%
  select(cod_prod, n, pgeo) %>%
  full_join(prod_may %>% select(-n) %>% rename_at('pgeo', ~'pgeo_may'),
             by = c("cod_prod")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_may))

prod_abr_adj$anio <- anio
prod_abr_adj$mes <- mes
prod_abr_adj$pgeo_may <- NULL

# Hacemos lo mismo con los meses anteriores

mes <- 3

bol_mar <- dbGetQuery(
  con,
  paste(
    "EXEC [dbo].[sp_get_calculos_precios_recolectados_mes]",
    anio, ",",
    mes
  )
)

bol_mar$precio_base <- with(
  bol_mar,
  precio_actual * cantidad_base / cantidad_actual
)

bol_mar <- bol_mar[
  !is.na(bol_mar$precio_base) &
  bol_mar$precio_base != 0,
]

bol_mar_sin_out <- bol_mar %>%
  left_join(
    dep_abr %>% select(codigo_articulo, Departamento, pgeo),
    by = c("codigo_articulo", "Departamento")
    ) %>%
  mutate(pgeo = coalesce(pgeo, precio_base)) %>%
  filter(precio_base <= pgeo * 1.25) %>%
  filter(precio_base >= pgeo / 1.25)

base_mar <- popular(bol_mar_sin_out)

write.xlsx(base_mar, "Boletas_IPC_marzo.xlsx")

dep_mar <- base_mar %>%
  group_by(codigo_articulo, Departamento) %>%
  summarize(
    pgeo = geo_mean(precio_base),
    n = n(),
    producto_nombre = first(producto_nombre),
    cant_base = first(cantidad_base),
    articulo = first(articulo),
    region = first(region),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  ) %>% 
  mutate(cod_prod = substr(codigo_articulo, 1, nchar(codigo_articulo) - 1)) %>%
  mutate(cod_prod = ifelse(cod_prod == "10400011", "1040001", cod_prod))

dep_mar_adj <- dep_mar %>%
  select(codigo_articulo, region, Departamento, n, pgeo) %>%
  full_join(dep_abr_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_abr'),
             by = c("codigo_articulo", "region", "Departamento")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_abr))

dep_mar_adj$anio <- anio
dep_mar_adj$mes <- mes
dep_mar_adj$pgeo_abr <- NULL

reg_mar <- dep_mar %>%
  group_by(codigo_articulo, region) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

reg_mar_adj <- reg_mar %>%
  select(codigo_articulo, region, n, pgeo) %>%
  full_join(reg_abr_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_abr'),
             by = c("codigo_articulo", "region")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_abr))

reg_mar_adj$anio <- anio
reg_mar_adj$mes <- mes
reg_mar_adj$pgeo_abr <- NULL

var_mar <- reg_mar %>%
  group_by(codigo_articulo) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

var_mar_adj <- var_mar %>%
  select(codigo_articulo, n, pgeo) %>%
  full_join(var_abr_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_abr'),
             by = c("codigo_articulo")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_abr))

var_mar_adj$anio <- anio
var_mar_adj$mes <- mes
var_mar_adj$pgeo_abr <- NULL

prod_mar <- var_mar %>%
  group_by(cod_prod) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

prod_mar_adj <- prod_mar %>%
  select(cod_prod, n, pgeo) %>%
  full_join(prod_abr_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_abr'),
             by = c("cod_prod")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_abr))

prod_mar_adj$anio <- anio
prod_mar_adj$mes <- mes
prod_mar_adj$pgeo_abr <- NULL

mes <- 2

bol_feb <- dbGetQuery(
  con,
  paste(
    "EXEC [dbo].[sp_get_calculos_precios_recolectados_mes]",
    anio, ",",
    mes
  )
)

bol_feb$precio_base <- with(
  bol_feb,
  precio_actual * cantidad_base / cantidad_actual
)

bol_feb <- bol_feb[
  !is.na(bol_feb$precio_base) &
  bol_feb$precio_base != 0,
]

bol_feb_sin_out <- bol_feb %>%
  left_join(
    dep_mar %>% select(codigo_articulo, Departamento, pgeo),
    by = c("codigo_articulo", "Departamento")
    ) %>%
  mutate(pgeo = coalesce(pgeo, precio_base)) %>%
  filter(precio_base <= pgeo * 1.25) %>%
  filter(precio_base >= pgeo / 1.25)

base_feb <- popular(bol_feb_sin_out)

write.xlsx(base_feb, "Boletas_IPC_febrero.xlsx")

dep_feb <- base_feb %>%
  group_by(codigo_articulo, Departamento) %>%
  summarize(
    pgeo = geo_mean(precio_base),
    n = n(),
    producto_nombre = first(producto_nombre),
    cant_base = first(cantidad_base),
    articulo = first(articulo),
    region = first(region),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  ) %>% 
  mutate(cod_prod = substr(codigo_articulo, 1, nchar(codigo_articulo) - 1)) %>%
  mutate(cod_prod = ifelse(cod_prod == "10400011", "1040001", cod_prod))

dep_feb_adj <- dep_feb %>%
  select(codigo_articulo, region, Departamento, n, pgeo) %>%
  full_join(dep_mar_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_mar'),
             by = c("codigo_articulo", "region", "Departamento")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_mar))

dep_feb_adj$anio <- anio
dep_feb_adj$mes <- mes
dep_feb_adj$pgeo_mar <- NULL

reg_feb <- dep_feb %>%
  group_by(codigo_articulo, region) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

reg_feb_adj <- reg_feb %>%
  select(codigo_articulo, region, n, pgeo) %>%
  full_join(reg_mar_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_mar'),
             by = c("codigo_articulo", "region")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_mar))

reg_feb_adj$anio <- anio
reg_feb_adj$mes <- mes
reg_feb_adj$pgeo_mar <- NULL

var_feb <- reg_feb %>%
  group_by(codigo_articulo) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

var_feb_adj <- var_feb %>%
  select(codigo_articulo, n, pgeo) %>%
  full_join(var_mar_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_mar'),
             by = c("codigo_articulo")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_mar))

var_feb_adj$anio <- anio
var_feb_adj$mes <- mes
var_feb_adj$pgeo_mar <- NULL

prod_feb <- var_feb %>%
  group_by(cod_prod) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

prod_feb_adj <- prod_feb %>%
  select(cod_prod, n, pgeo) %>%
  full_join(prod_mar_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_mar'),
             by = c("cod_prod")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_mar))

prod_feb_adj$anio <- anio
prod_feb_adj$mes <- mes
prod_feb_adj$pgeo_mar <- NULL

mes <- 1

bol_ene <- dbGetQuery(
  con,
  paste(
    "EXEC [dbo].[sp_get_calculos_precios_recolectados_mes]",
    anio, ",",
    mes
  )
)

# Enero y diciembre aun muestran variedades de la base 2010 en el sistema
bol_ene <- bol_ene %>% filter(tipo_precio == "IPC-2023")

bol_ene$precio_base <- with(
  bol_ene,
  precio_actual * cantidad_base / cantidad_actual
)

bol_ene <- bol_ene[
  !is.na(bol_ene$precio_base) &
  bol_ene$precio_base != 0,
]

bol_ene_sin_out <- bol_ene %>%
  left_join(
    dep_feb %>% select(codigo_articulo, Departamento, pgeo),
    by = c("codigo_articulo", "Departamento")
    ) %>%
  mutate(pgeo = coalesce(pgeo, precio_base)) %>%
  filter(precio_base <= pgeo * 1.25) %>%
  filter(precio_base >= pgeo / 1.25)

base_ene <- popular(bol_ene_sin_out)

write.xlsx(base_ene, "Boletas_IPC_enero.xlsx")

dep_ene <- base_ene %>%
  group_by(codigo_articulo, Departamento) %>%
  summarize(
    pgeo = geo_mean(precio_base),
    n = n(),
    producto_nombre = first(producto_nombre),
    cant_base = first(cantidad_base),
    articulo = first(articulo),
    region = first(region),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  ) %>% 
  mutate(cod_prod = substr(codigo_articulo, 1, nchar(codigo_articulo) - 1)) %>%
  mutate(cod_prod = ifelse(cod_prod == "10400011", "1040001", cod_prod))

dep_ene_adj <- dep_ene %>%
  select(codigo_articulo, region, Departamento, n, pgeo) %>%
  full_join(dep_feb_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_feb'),
             by = c("codigo_articulo", "region", "Departamento")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_feb))

dep_ene_adj$anio <- anio
dep_ene_adj$mes <- mes
dep_ene_adj$pgeo_feb <- NULL

reg_ene <- dep_ene %>%
  group_by(codigo_articulo, region) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

reg_ene_adj <- reg_ene %>%
  select(codigo_articulo, region, n, pgeo) %>%
  full_join(reg_feb_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_feb'),
             by = c("codigo_articulo", "region")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_feb))

reg_ene_adj$anio <- anio
reg_ene_adj$mes <- mes
reg_ene_adj$pgeo_feb <- NULL

var_ene <- reg_ene %>%
  group_by(codigo_articulo) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

var_ene_adj <- var_ene %>%
  select(codigo_articulo, n, pgeo) %>%
  full_join(var_feb_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_feb'),
             by = c("codigo_articulo")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_feb))

var_ene_adj$anio <- anio
var_ene_adj$mes <- mes
var_ene_adj$pgeo_feb <- NULL

prod_ene <- var_ene %>%
  group_by(cod_prod) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

prod_ene_adj <- prod_ene %>%
  select(cod_prod, n, pgeo) %>%
  full_join(prod_feb_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_feb'),
             by = c("cod_prod")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_feb))

prod_ene_adj$anio <- anio
prod_ene_adj$mes <- mes
prod_ene_adj$pgeo_feb <- NULL

anio <- 2023
mes <- 12

bol_dic <- dbGetQuery(
  con,
  paste(
    "EXEC [dbo].[sp_get_calculos_precios_recolectados_mes]",
    anio, ",",
    mes
  )
)

bol_dic <- bol_dic %>% filter(tipo_precio == "IPC-2023")

bol_dic$precio_base <- with(
  bol_dic,
  precio_actual * cantidad_base / cantidad_actual
)

bol_dic <- bol_dic[
  !is.na(bol_dic$precio_base) &
  bol_dic$precio_base != 0,
]

bol_dic_sin_out <- bol_dic %>%
  left_join(
    dep_ene %>% select(codigo_articulo, Departamento, pgeo),
    by = c("codigo_articulo", "Departamento")
    ) %>%
  mutate(pgeo = coalesce(pgeo, precio_base)) %>%
  filter(precio_base <= pgeo * 1.25) %>%
  filter(precio_base >= pgeo / 1.25)

base_dic <- popular(bol_dic_sin_out)

write.xlsx(base_dic, "Boletas_IPC_diciembre.xlsx")

dep_dic <- base_dic %>%
  group_by(codigo_articulo, Departamento) %>%
  summarize(
    pgeo = geo_mean(precio_base),
    n = n(),
    producto_nombre = first(producto_nombre),
    cant_base = first(cantidad_base),
    articulo = first(articulo),
    region = first(region),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  ) %>% 
  mutate(cod_prod = substr(codigo_articulo, 1, nchar(codigo_articulo) - 1)) %>%
  mutate(cod_prod = ifelse(cod_prod == "10400011", "1040001", cod_prod))

dep_dic_adj <- dep_dic %>%
  select(codigo_articulo, region, Departamento, n, pgeo) %>%
  full_join(dep_ene_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_ene'),
             by = c("codigo_articulo", "region", "Departamento")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ene))

dep_dic_adj$anio <- anio
dep_dic_adj$mes <- mes
dep_dic_adj$pgeo_ene <- NULL

reg_dic <- dep_dic %>%
  group_by(codigo_articulo, region) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

reg_dic_adj <- reg_dic %>%
  select(codigo_articulo, region, n, pgeo) %>%
  full_join(reg_ene_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_ene'),
             by = c("codigo_articulo", "region")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ene))

reg_dic_adj$anio <- anio
reg_dic_adj$mes <- mes
reg_dic_adj$pgeo_ene <- NULL

var_dic <- reg_dic %>%
  group_by(codigo_articulo) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    cod_prod = first(cod_prod),
    cant_base = first(cant_base),
    articulo = first(articulo),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

var_dic_adj <- var_dic %>%
  select(codigo_articulo, n, pgeo) %>%
  full_join(var_ene_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_ene'),
             by = c("codigo_articulo")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ene))

var_dic_adj$anio <- anio
var_dic_adj$mes <- mes
var_dic_adj$pgeo_ene <- NULL

prod_dic <- var_dic %>%
  group_by(cod_prod) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

prod_dic_adj <- prod_dic %>%
  select(cod_prod, n, pgeo) %>%
  full_join(prod_ene_adj %>% select(-n) %>% rename_at('pgeo', ~'pgeo_ene'),
             by = c("cod_prod")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ene))

prod_dic_adj$anio <- anio
prod_dic_adj$mes <- mes
prod_dic_adj$pgeo_ene <- NULL

# Ahora imputamos los precios promedio al mes siguiente, empezando en enero

dep_feb_imp <- dep_ene_adj %>%
  select(codigo_articulo, region, Departamento, pgeo) %>%
  rename_at('pgeo', ~'pgeo_ene') %>%
  full_join(
    dep_feb_adj,
    by = c("codigo_articulo", "region", "Departamento")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ene))

dep_feb_imp$mes <- 2
dep_feb_imp$anio <- 2024
dep_feb_imp$pgeo_ene <- NULL

dep_mar_imp <- dep_feb_imp %>%
  select(codigo_articulo, region, Departamento, pgeo) %>%
  rename_at('pgeo', ~'pgeo_feb') %>%
  full_join(
    dep_mar_adj,
    by = c("codigo_articulo", "region", "Departamento")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_feb))

dep_mar_imp$mes <- 3
dep_mar_imp$anio <- 2024
dep_mar_imp$pgeo_feb <- NULL

dep_abr_imp <- dep_mar_imp %>%
  select(codigo_articulo, region, Departamento, pgeo) %>%
  rename_at('pgeo', ~'pgeo_mar') %>%
  full_join(
    dep_abr_adj,
    by = c("codigo_articulo", "region", "Departamento")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_mar))

dep_abr_imp$mes <- 4
dep_abr_imp$anio <- 2024
dep_abr_imp$pgeo_mar <- NULL

dep_may_imp <- dep_abr_imp %>%
  select(codigo_articulo, region, Departamento, pgeo) %>%
  rename_at('pgeo', ~'pgeo_abr') %>%
  full_join(
    dep_may,
    by = c("codigo_articulo", "region", "Departamento")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_abr))

dep_may_imp$mes <- 5
dep_may_imp$anio <- 2024
dep_may_imp$pgeo_abr <- NULL

reg_feb_imp <- reg_ene_adj %>%
  select(codigo_articulo, region, pgeo) %>%
  rename_at('pgeo', ~'pgeo_ene') %>%
  full_join(
    reg_feb_adj,
    by = c("codigo_articulo", "region")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ene))

reg_feb_imp$mes <- 2
reg_feb_imp$anio <- 2024
reg_feb_imp$pgeo_ene <- NULL

reg_mar_imp <- reg_feb_imp %>%
  select(codigo_articulo, region, pgeo) %>%
  rename_at('pgeo', ~'pgeo_feb') %>%
  full_join(
    reg_mar_adj,
    by = c("codigo_articulo", "region")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_feb))

reg_mar_imp$mes <- 3
reg_mar_imp$anio <- 2024
reg_mar_imp$pgeo_feb <- NULL

reg_abr_imp <- reg_mar_imp %>%
  select(codigo_articulo, region, pgeo) %>%
  rename_at('pgeo', ~'pgeo_mar') %>%
  full_join(
    reg_abr_adj,
    by = c("codigo_articulo", "region")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_mar))

reg_abr_imp$mes <- 4
reg_abr_imp$anio <- 2024
reg_abr_imp$pgeo_mar <- NULL

reg_may_imp <- reg_abr_imp %>%
  select(codigo_articulo, region, pgeo) %>%
  rename_at('pgeo', ~'pgeo_abr') %>%
  full_join(
    reg_may,
    by = c("codigo_articulo", "region")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_abr))

reg_may_imp$mes <- 5
reg_may_imp$anio <- 2024
reg_may_imp$pgeo_abr <- NULL

var_feb_imp <- var_ene_adj %>%
  select(codigo_articulo, pgeo) %>%
  rename_at('pgeo', ~'pgeo_ene') %>%
  full_join(
    var_feb_adj,
    by = c("codigo_articulo")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ene))

var_feb_imp$mes <- 2
var_feb_imp$anio <- 2024
var_feb_imp$pgeo_ene <- NULL

var_mar_imp <- var_feb_imp %>%
  select(codigo_articulo, pgeo) %>%
  rename_at('pgeo', ~'pgeo_feb') %>%
  full_join(
    var_mar_adj,
    by = c("codigo_articulo")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_feb))

var_mar_imp$mes <- 3
var_mar_imp$anio <- 2024
var_mar_imp$pgeo_feb <- NULL

var_abr_imp <- var_mar_imp %>%
  select(codigo_articulo, pgeo) %>%
  rename_at('pgeo', ~'pgeo_mar') %>%
  full_join(
    var_abr_adj,
    by = c("codigo_articulo")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_mar))

var_abr_imp$mes <- 4
var_abr_imp$anio <- 2024
var_abr_imp$pgeo_mar <- NULL

var_may_imp <- var_abr_imp %>%
  select(codigo_articulo, pgeo) %>%
  rename_at('pgeo', ~'pgeo_abr') %>%
  full_join(
    var_may,
    by = c("codigo_articulo")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_abr))

var_may_imp$mes <- 5
var_may_imp$anio <- 2024
var_may_imp$pgeo_abr <- NULL

prod_feb_imp <- prod_ene_adj %>%
  select(cod_prod, pgeo) %>%
  rename_at('pgeo', ~'pgeo_ene') %>%
  full_join(
    prod_feb_adj,
    by = c("cod_prod")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ene))

prod_feb_imp$mes <- 2
prod_feb_imp$anio <- 2024
prod_feb_imp$pgeo_ene <- NULL

prod_mar_imp <- prod_feb_imp %>%
  select(cod_prod, pgeo) %>%
  rename_at('pgeo', ~'pgeo_feb') %>%
  full_join(
    prod_mar_adj,
    by = c("cod_prod")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_feb))

prod_mar_imp$mes <- 3
prod_mar_imp$anio <- 2024
prod_mar_imp$pgeo_feb <- NULL

prod_abr_imp <- prod_mar_imp %>%
  select(cod_prod, pgeo) %>%
  rename_at('pgeo', ~'pgeo_mar') %>%
  full_join(
    prod_abr_adj,
    by = c("cod_prod")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_mar))

prod_abr_imp$mes <- 4
prod_abr_imp$anio <- 2024
prod_abr_imp$pgeo_mar <- NULL

prod_may_imp <- prod_abr_imp %>%
  select(cod_prod, pgeo) %>%
  rename_at('pgeo', ~'pgeo_abr') %>%
  full_join(
    prod_may,
    by = c("cod_prod")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_abr))

prod_may_imp$mes <- 5
prod_may_imp$anio <- 2024
prod_may_imp$pgeo_abr <- NULL

# Concatenamos los resultados según cada grupo

PP_dep <- rbind(
  dep_may_imp, dep_abr_imp, dep_mar_imp, dep_feb_imp, dep_ene_adj, dep_dic_adj
)
PP_reg <- rbind(
  reg_may_imp, reg_abr_imp, reg_mar_imp, reg_feb_imp, reg_ene_adj, reg_dic_adj
)
PP_var <- rbind(
  var_may_imp, var_abr_imp, var_mar_imp, var_feb_imp, var_ene_adj, var_dic_adj
)
PP_prod <- rbind(
  prod_may_imp, prod_abr_imp, prod_mar_imp, prod_feb_imp, prod_ene_adj,
  prod_dic_adj
)

# Algunas celdas quedaron vacías. Como todas son determinadas por el código del
# artículo, podemos crear un df con la información

inf_articulos <- PP_dep %>%
  select(codigo_articulo, articulo, cant_base, cod_prod, producto_nombre) %>%
  drop_na() %>% distinct()

# Y luego añadirlas

PP_dep_fill <- PP_dep %>%
  select(codigo_articulo, region, Departamento, pgeo, mes, anio) %>%
  left_join(inf_articulos, by = c("codigo_articulo"))

PP_reg_fill <- PP_reg %>%
  select(codigo_articulo, region, pgeo, mes, anio) %>%
  left_join(inf_articulos, by = c("codigo_articulo"))

PP_var_fill <- PP_var %>%
  select(codigo_articulo, pgeo, mes, anio) %>%
  left_join(inf_articulos, by = c("codigo_articulo"))

PP_prod_fill <- PP_prod %>%
  select(cod_prod, pgeo, mes, anio) %>%
  left_join(inf_articulos %>% select(cod_prod, producto_nombre) %>% distinct(),
            by = c("cod_prod")
  )

# Guardamos los resultados
wb <- createWorkbook()

addWorksheet(wb, "Precios_promedio_dep")
addWorksheet(wb, "Precios_promedio_reg")
addWorksheet(wb, "Precios_promedio_var")
addWorksheet(wb, "Precios_promedio_prod")

writeData(wb, "Precios_promedio_dep", PP_dep_fill)
writeData(wb, "Precios_promedio_reg", PP_reg_fill)
writeData(wb, "Precios_promedio_var", PP_var_fill)
writeData(wb, "Precios_promedio_prod", PP_prod_fill)

saveWorkbook(wb, "Precios_promedio_IPC_mayo.xlsx", overwrite = TRUE)