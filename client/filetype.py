from magic import Magic
import xml.sax
import csv

TYPE_MAPPING = {"gzip":"gz", "bzip2":"bz2", "Zip":"zip", "RAR":"rar", "POSIX tar":"tar"}
COMPRESSION = ["gz", "bz2"]
ARCHIVED = ["zip", "rar", "tar"]

class FileType:

    def __init__(self):
        self.m = Magic()

    def get_type(self, fname):
        ftype = self.m.from_file(fname)

        for k in TYPE_MAPPING.keys():
            if k in ftype:
                return TYPE_MAPPING[k]

        #solutions here from http://stackoverflow.com/questions/9084228/python-to-check-if-a-gzipped-file-is-xml-or-csv
        #and http://stackoverflow.com/questions/2984888/check-if-file-has-a-csv-format-with-python
        if 'text' in ftype:

            with open(fname, 'rb') as fh:

                try:
                    xml.sax.parse(fh, xml.sax.ContentHandler())
                    return 'xml'
                except: # SAX' exceptions are not public
                    pass

                fh.seek(0)
			
                try:
                    dialect = csv.Sniffer().sniff(fh.read(1024))
                    return 'csv'
                except csv.Error:
                    pass

            return 'txt'

    def is_compression(self, fname):
        ftype = self.get_type(fname)
        return self.is_compression_by_type(ftype)

    def is_compression_by_type(self, ftype):
        if ftype in COMPRESSION:
            return True
        return False
	
    def is_archived(self, fname):
        ftype = self.get_type(fname)
        return self.is_archived_by_type(ftype)

    def is_archived_by_type(self, ftype):
        if ftype in ARCHIVED:
            return True
        return False

if __name__ == '__main__':
    ft = FileType()
    ftype = ft.get_type('test.zip')
    print ftype
    print "compression? " + str(ft.is_compression(ftype))
    print "archived? " + str(ft.is_archived(ftype))
