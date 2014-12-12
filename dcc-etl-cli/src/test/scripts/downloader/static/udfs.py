# ===========================================================================

# Reproduces logic from org.icgc.dcc.etl.loader.cascading.PreProcessFunction#trimAndTruncateValues()
# truncate_mutation("ACTCAAGGTTTGTGTCATTAAATCTTTAGTTACTGAATTGGGGCTCTGCTTCGTTGCCATTAAGCCAGTCTGGCTGAGATCCCCCTGCTTTCCTCTCTCCCTGCTTACTTGTCAGGCTACCTTTTGCTCCATTTTCTGCTCACTCCTCCTAATGGCTTGGTGAAATAGCAAACAAGCCACCAGCAGGAATCTAGTCTGGATGACTGCTTCTGGAGCCTGGATGCAGTACCATTCTTCCACTGATTCAGTGAGTAACTGTTAGGTGGTTCCCTAAGGGATTAGGTATTTCATCACTGAGCTAACCCTGGCTATCATTCTGCTTTTCTTGGCTGTCTTTCAGATTTGACTTTATTTCTAAAAATATTTCAATGGGTCATATCACAGATTCTTTTTTTTTAAATTAAAGTAACATTTCCAATCTACTAATGCTAATACTGTTTCGTATTTATAGCTGATTTGATGGAGTTGGACATGGCCATGGAACCAGACAGAAAAGCGGCTGTTAGTCACTGGCAGCAACAGTCTTACCTGGACTCTGGAATCCATTCTGGTGCCACTACCACAGCTCCTTCTCTGAGTGGTAAAGGCAATCCTGAGGAAGAGGATGTGGATACCTCCCAAGTCCTGTATGAGTGGGAACAGGGATTTTCTCAGTCCTTCACTCAAGAACAAGTAGCTGGTAAGAGTATTATTTTTCATTGCCTTACTGAAAGTCAGAATGCAGTTTTGAGAACTAAAAAGTTAGTGTATAATAGTTTAAATAAAATGTTGTGGTGAAGAAAAGAGAGTAATAGCAATGTCACTTTTACCATTTAGGATAGCAAATACTTAGGTAAATGCTGAACTGTGGATAGTGAGTGTTGAATTAACCTTTTCCAGATATTGATGGACAGTATGCAATGACTCGAGCTCAGAGGGTACGAGCTGCTATGTTCCCTGAGACATTAGATGAGGGCATGCAGATCCCATCTACACAGTTTGATGCTGCTCATCCCACTAATGTCCAGCGTTTGGCT>-") # returns: "ACTCAAGGTTTGTGTCATTAAATCTTTAGTTACTGAATTGGGGCTCTGCTTCGTTGCCATTAAGCCAGTCTGGCTGAGATCCCCCTGCTTTCCTCTCTCCCTGCTTACTTGTCAGGCTACCTTTTGCTCCATTTTCTGCTCACTCCTCCTAATGGCTTGGTGAAATAGCAAACAAGCCACCAGCAGGAATCTAGTCTGGATGACTGCTTCTGGAGCCTGGATGCAGTACCATTCTTCCACTGATTCAGTGAGTAACTGTTAGGTGGTTCCCTAAGGGATTAGGTATTTCATCACTGAGCTAACCCTGGCTATCATTCTGCTTTTCTTGGCTGTCTTTCAGATTTGACTTTATTTCTAAAAATATTTCAATGGGTCATATCACAGATTCTTTTTTTTTAAATTA [... truncated]"
@outputSchema("truncation: chararray")
def truncate_mutation(mutation):
  
  # TODO: find a way to declare it outside the scope of this function (jython didn't like it)
  THRESHOLD = 403
  MESSAGE = " [... truncated]"

  if mutation is not None and len(mutation) > THRESHOLD:
    mutation = mutation[:THRESHOLD] + MESSAGE
  return mutation

# ---------------------------------------------------------------------------

@outputSchema("cleared: chararray")
def clear(value):
  if value is None or value.strip() == "" or value == "-777" or value == "-888" or value == "-999" or value == "-9999":
    value = None # Must use None here, not "" because otherwise distinct will fail to equate None from left joins with "" otherwise
  return value

# ===========================================================================
