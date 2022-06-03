class ReadDataUtil:
    # def __init__(self):

    def readCsv(self,spark,path,schema=None,inferschema=True,header=True,sep=","):
        """
        Returns new dataframe by reading provided csv file
        :param spark: spark session
        :param path: csv file path or directory path
        :param schema: provide schema,requried when inferschema is false
        :param inferschema: if ture :detect file schema else false:ignore outo detect schema
        :param header: if ture: input csv file has header
        :param sep: default: "," specify separater present in csv file
        :return:
        """
        if (inferschema is False) and (schema==None):
            raise Exception("Please provide inferschema as true else provide schema for given input file ")

        if schema == None:
            readdf = spark.read.csv(path=path, inferSchema=inferschema, header=header, sep=sep)

        else:
            readdf = spark.read.csv(path=path, schema=schema, header=header, sep=sep)

        return readdf