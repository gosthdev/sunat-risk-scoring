import { assertEquals } from "jsr:@std/assert@1";
import { deducirPeriodo } from "./periodo.ts";

Deno.test("deducirPeriodo", () => {
  assertEquals(deducirPeriodo("PadronRUC_202512.csv"), "2025-12");
  assertEquals(deducirPeriodo("PadronRUC_202501.zip"), "2025-01");
  assertEquals(deducirPeriodo("ORDENESCOMPRAENERO2025_0.xlsx"), "2025-01");
  assertEquals(deducirPeriodo("ordenescompraSETIEMBRE2024.xlsx"), "2024-09");
  assertEquals(deducirPeriodo("ORDENESCOMPRASEPTIEMBRE2024.xlsx"), "2024-09");
  assertEquals(deducirPeriodo("PadronRUC_202513.csv"), null);
  assertEquals(deducirPeriodo("datos.csv"), null);
  assertEquals(deducirPeriodo("PadronRUC_190412.csv"), null);
  assertEquals(deducirPeriodo("ruc_20123456789.csv"), null);
  assertEquals(deducirPeriodo("PadronRUC_20251201.csv"), null);
  assertEquals(deducirPeriodo("ORDENESCOMPRASÉPTIEMBRE2024.xlsx"), "2024-09");
  assertEquals(deducirPeriodo("PadronRUC_202501_ENERO2025.csv"), "2025-01");
  assertEquals(deducirPeriodo("PadronRUC_202501_FEBRERO2025.csv"), null);
});
