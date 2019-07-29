import xml.etree.ElementTree as ET

ns = {'mes': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message',
      'str': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure'}


def getKeyfamilies(bisdsd):
  root = ET.parse(bisdsd).getroot()
  dict_kf = dict()
  for kf in root.find("mes:KeyFamilies", ns):
    _temp = dict()
    _temp['agencyid'] = kf.attrib['agencyID']
    _temp['urn'] = kf.attrib['urn']
    _temp['version'] = kf.attrib['version']
    _temp['name'] = kf.find('str:Name',ns)
    _temp['dimension'] = [
      x.attrib['codelist'] for x in 
      root.find(".//str:KeyFamily[@id='{}']".format(kf.attrib['id']),ns).
      find("str:Components", ns).
      findall("str:Dimension", ns)
    ]
    _temp['concept'] = [
      x.attrib['conceptRef'] for x in 
      root.find(".//str:KeyFamily[@id='{}']".format(kf.attrib['id']),ns).
      find("str:Components", ns).
      findall("str:Dimension", ns)
    ]
    dict_kf[kf.attrib['id']] = _temp
  return dict_kf


def getCodelist(bisdsd):
  root = ET.parse(bisdsd).getroot()
  dict_cl = dict()
  for cl in root.find("mes:CodeLists", ns):
    _temp = {'name': cl.find("str:Name", ns).text}
    _temp2 = dict()
    for clval in cl.findall("str:Code", ns):
      _temp2[clval.attrib['value']] = clval.find("str:Description", ns).text
    _temp['code'] = _temp2
    dict_cl[cl.attrib['id']] = _temp
  return dict_cl


if __name__ == "__main__":
  getCodelist()