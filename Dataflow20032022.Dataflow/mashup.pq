[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silverBrasileirao20032022_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared silverBrasileirao20032022 = let
  Source = Excel.Workbook(Web.Contents("https://lodenestor-my.sharepoint.com/personal/emilianoregazzoni_lodenestor_onmicrosoft_com/Documents/Apps/Microsoft Power Query/Uploaded Files/brasileirao2.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "brasileirao", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"year", Int64.Type}, {"position", Int64.Type}, {"team", type text}, {"points", Int64.Type}, {"games", Int64.Type}, {"victories", Int64.Type}, {"draws", Int64.Type}, {"losses", Int64.Type}, {"goals_scored", Int64.Type}, {"goals_against", Int64.Type}, {"goals_difference", Int64.Type}, {"total_points", Int64.Type}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "teamName", each [team]), {{"teamName", type text}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added custom", {"year", "position", "team", "teamName", "points", "games", "victories", "draws", "losses", "goals_scored", "goals_against", "goals_difference", "total_points"}),
  Custom = Table.TransformColumns(
    #"Reordered columns",
    {
        {
            "teamName",
            each
                if _ = null then
                    null
                else
                    List.Accumulate(
                        {
                            {"á", "a"}, {"à", "a"}, {"â", "a"}, {"ã", "a"}, {"ä", "a"},
                            {"é", "e"}, {"è", "e"}, {"ê", "e"}, {"ë", "e"},
                            {"í", "i"}, {"ì", "i"}, {"î", "i"}, {"ï", "i"},
                            {"ó", "o"}, {"ò", "o"}, {"ô", "o"}, {"õ", "o"}, {"ö", "o"},
                            {"ú", "u"}, {"ù", "u"}, {"û", "u"}, {"ü", "u"},
                            {"ç", "c"},

                            {"Á", "A"}, {"À", "A"}, {"Â", "A"}, {"Ã", "A"}, {"Ä", "A"},
                            {"É", "E"}, {"È", "E"}, {"Ê", "E"}, {"Ë", "E"},
                            {"Í", "I"}, {"Ì", "I"}, {"Î", "I"}, {"Ï", "I"},
                            {"Ó", "O"}, {"Ò", "O"}, {"Ô", "O"}, {"Õ", "O"}, {"Ö", "O"},
                            {"Ú", "U"}, {"Ù", "U"}, {"Û", "U"}, {"Ü", "U"},
                            {"Ç", "C"}
                        },
                        Text.Trim(_),
                        (textoActual, reemplazo) =>
                            Text.Replace(
                                textoActual,
                                reemplazo{0},
                                reemplazo{1}
                            )
                    ),
            type text
        }
    }
)
in
  Custom;
shared silverBrasileirao20032022_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "151fcb69-dedf-4d5c-9f1f-0638abe5dfc1"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "490b5693-ab01-4afb-abc0-077f72fda460"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "silverBrasileirao20032022", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
