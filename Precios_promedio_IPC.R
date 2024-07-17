library("ini")
library("odbc")
library("openxlsx")
library("dplyr")

# El número se cambia dependiendo del mes
anio <- 2024
mes <- 6

# El archivo con los meses anteriores. Se espera un archivo de Excel con 4
# hojas para los precios por departamente, región, variedad y producto.
meses_anteriores <- "Precios_promedio_IPC_mayo.xlsx"

popular <- function(boletas) {

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
  
  counts <- boletas %>%
    group_by(codigo_articulo, Departamento, cantidad_actual) %>%
    summarize(count = n(), .groups = 'drop')
  
  max_counts <- counts %>%
    group_by(codigo_articulo, Departamento) %>%
    summarize(max_count = max(count), .groups = 'drop')
  
  most_common <- counts %>%
    inner_join(max_counts, by = c("codigo_articulo", "Departamento")) %>%
    filter(count == max_count) %>%
    select(-count, -max_count)
  
  common <- common %>%
    inner_join(
      most_common,
      by = c("codigo_articulo", "Departamento", "cantidad_actual")
    )
  
  return(rbind(common, uncommon))
}

geo_mean <- function(x) {
  exp(mean(log(x[x > 0]), na.rm = TRUE))
}

w_geo_mean <- function(x, w) {
  exp(sum(w * log(x[x > 0]), na.rm = TRUE) / sum(w, na.rm = TRUE))
}

config <- ini::read.ini("config.ini")

server <- config$database$server
user <- config$database$user
password <- config$database$password
database <- config$database$database

conn_str <- paste0(
  "Driver={ODBC Driver 17 for SQL Server};",
  "Server=", server, ";",
  "Database=", database, ";",
  "Uid=", user, ";",
  "Pwd=", password, ";"
)

con <- dbConnect(odbc::odbc(), .connection_string = conn_str)

boletas <- dbGetQuery(
  con,
  paste(
    "EXEC [dbo].[sp_get_calculos_precios_recolectados_mes]",
    anio, ",",
    mes
  )
)

dbDisconnect(con)

boletas$precio_base <- with(
  boletas,
  precio_actual * cantidad_base / cantidad_actual
)

boletas <- boletas[
  !is.na(boletas$precio_base) &
  boletas$precio_base != 0,
]

# Tomamos los precios por departamento del mes anterior para comparar.
mes_ant <- mes - 1 + 12 * (mes == 1)
anio_mes_ant <- anio - (mes == 1)

dep_ant <- read.xlsx(meses_anteriores, "Precios_promedio_dep") %>%
  filter(mes == mes_ant) %>% filter(anio == anio_mes_ant)
  
inf_articulos <- dep_ant %>%
  select(codigo_articulo, articulo, cant_base, cod_prod, producto_nombre) %>%
  distinct()

dep_ant <- dep_ant %>%
  select(codigo_articulo, region, Departamento, pgeo) %>%
  rename_at('pgeo', ~'pgeo_ant')

# Retiramos las boletas con variación mayor a 25% positiva o negativa.
bol_sin_out <- boletas %>%
  left_join(
    dep_ant,
    by = c("codigo_articulo", "region", "Departamento")
    ) %>%
  filter(precio_base <= pgeo_ant * 1.25) %>%
  filter(precio_base >= pgeo_ant / 1.25)

base <- popular(bol_sin_out)

write.xlsx(base, paste0("Boletas_IPC_", anio, "-", mes, ".xlsx"))

dep <- base %>%
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

reg <- dep %>%
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

var <- reg %>%
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

prod <- var %>%
  group_by(cod_prod) %>%
  summarize(
    pgeo = w_geo_mean(pgeo, n),
    n = sum(n),
    producto_nombre = first(producto_nombre),
    mes = first(mes),
    anio = first(anio),
    .groups = "drop"
  )

reg_ant <- read.xlsx(meses_anteriores, "Precios_promedio_reg") %>%
  filter(mes == mes_ant) %>% filter(anio == anio_mes_ant) %>%
  select(codigo_articulo, region, pgeo) %>%
  rename_at('pgeo', ~'pgeo_ant')

var_ant <- read.xlsx(meses_anteriores, "Precios_promedio_var") %>%
  filter(mes == mes_ant) %>% filter(anio == anio_mes_ant) %>%
  select(codigo_articulo, pgeo) %>%
  rename_at('pgeo', ~'pgeo_ant')

prod_ant <- read.xlsx(meses_anteriores, "Precios_promedio_prod") %>%
  filter(mes == mes_ant) %>% filter(anio == anio_mes_ant) %>%
  select(cod_prod, pgeo) %>%
  rename_at('pgeo', ~'pgeo_ant')

dep_imp <- dep_ant %>%
  full_join(dep, by = c("codigo_articulo", "region", "Departamento")) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ant))

dep_imp$mes <- mes
dep_imp$anio <- anio
dep_imp$pgeo_ant <- NULL

reg_imp <- reg_ant %>%
  full_join(dep, by = c("codigo_articulo", "region")) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ant))

reg_imp$mes <- mes
reg_imp$anio <- anio
reg_imp$pgeo_ant <- NULL

var_imp <- var_ant %>%
  full_join(
    dep,
    by = c("codigo_articulo")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ant))

var_imp$mes <- mes
var_imp$anio <- anio
var_imp$pgeo_ant <- NULL

prod_imp <- prod_ant %>%
  full_join(
    dep,
    by = c("cod_prod")
  ) %>%
  mutate(pgeo = coalesce(pgeo, pgeo_ant))

prod_imp$mes <- mes
prod_imp$anio <- anio
prod_imp$pgeo_ant <- NULL

dep_fill <- dep_imp %>%
  select(codigo_articulo, region, Departamento, pgeo, mes, anio) %>%
  left_join(inf_articulos, by = c("codigo_articulo"))

reg_fill <- reg_imp %>%
  select(codigo_articulo, region, pgeo, mes, anio) %>%
  left_join(inf_articulos, by = c("codigo_articulo"))

var_fill <- var_imp %>%
  select(codigo_articulo, pgeo, mes, anio) %>%
  left_join(inf_articulos, by = c("codigo_articulo"))

prod_fill <- prod_imp %>%
  select(cod_prod, pgeo, mes, anio) %>%
  left_join(
    inf_articulos %>% select(cod_prod, producto_nombre) %>% distinct(),
    by = c("cod_prod")
  )

# Guardamos los resultados
wb <- createWorkbook()

addWorksheet(wb, "Precios_promedio_dep")
addWorksheet(wb, "Precios_promedio_reg")
addWorksheet(wb, "Precios_promedio_var")
addWorksheet(wb, "Precios_promedio_prod")

writeData(wb, "Precios_promedio_dep", dep_fill)
writeData(wb, "Precios_promedio_reg", reg_fill)
writeData(wb, "Precios_promedio_var", var_fill)
writeData(wb, "Precios_promedio_prod", prod_fill)

saveWorkbook(
  wb,
  paste0("Precios_promedio_IPC_", anio, "-", mes, ".xlsx"),
  overwrite = TRUE
)