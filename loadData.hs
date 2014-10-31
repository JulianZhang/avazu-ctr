
import Text.CSV.Lazy.String




usedChar = choice [letter,digit,(char '_'),(char '-')]

csvSep =  sepBy (many1 usedChar) (char ',')                 

loadTestData = do
	testFile <- readFile "data/test_rev2"
	let cfile = parseCSV testFile
	let coloum = head cfile
	let body = tail cfile 
	return $ head $ fromCSVTable $ csvTable  body

test = parse csvSep "fail" "sfsdf,sdfsdf,sdfsdf,12312,234d"
