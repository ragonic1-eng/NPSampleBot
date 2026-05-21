"use client";

import {
  Document,
  Image,
  Page,
  StyleSheet,
  Text,
  View,
} from "@react-pdf/renderer";
import {
  COMPANY_ADDRESS_LINES,
  DEFAULT_LEAD_COMMENT,
  priceLabel,
  QuoteData,
  SALES_PEOPLE,
  todayLabel,
} from "./constants";

// Style sheet — A4 page (210 x 297mm). Sizes in pt: 1mm ≈ 2.835pt.
const styles = StyleSheet.create({
  page: {
    paddingTop: 14 * 2.835,
    paddingBottom: 18 * 2.835,
    paddingLeft: 18 * 2.835,
    paddingRight: 18 * 2.835,
    fontSize: 11,
    fontFamily: "Helvetica",
    color: "#222",
  },

  // ---- Header band: logos + address.
  header: {
    flexDirection: "row",
    alignItems: "center",
    marginBottom: 4 * 2.835,
  },
  logoNP: { width: 28 * 2.835, height: 28 * 2.835 },
  logoBSI: { width: 22 * 2.835, height: 26 * 2.835 },
  addressBlock: {
    flex: 1,
    paddingLeft: 6 * 2.835,
    paddingRight: 6 * 2.835,
    flexDirection: "column",
  },
  addrLineBold: { fontSize: 11, fontFamily: "Helvetica-Bold", marginBottom: 2 },
  addrLine:     { fontSize: 9, marginBottom: 1 },

  // ---- Misc.
  date: { textAlign: "right", fontSize: 10, marginBottom: 4 * 2.835 },

  customerBlock: { marginBottom: 4 * 2.835 },
  custCompany:   { fontSize: 11, fontFamily: "Helvetica-Bold", marginBottom: 2 },
  custLine:      { fontSize: 11, marginBottom: 2 },

  greeting: { fontSize: 11, marginBottom: 2 * 2.835 },

  title: {
    fontSize: 12,
    fontFamily: "Helvetica-Bold",
    textDecoration: "underline",
    marginBottom: 2 * 2.835,
  },

  body: { fontSize: 11, marginBottom: 2 },
  comment: { marginBottom: 4 * 2.835 },

  // ---- Product table.
  table: { marginBottom: 4 * 2.835 },
  tableRow: { flexDirection: "row" },
  tableHeaderCell: {
    backgroundColor: "#E25822",
    color: "#FFFFFF",
    fontFamily: "Helvetica-Bold",
    fontSize: 10,
    padding: 5,
    borderColor: "#888",
    borderStyle: "solid",
    borderWidth: 0.5,
    textAlign: "center",
  },
  tableCell: {
    fontSize: 10,
    padding: 5,
    borderColor: "#888",
    borderStyle: "solid",
    borderWidth: 0.5,
    textAlign: "center",
  },
  colName: { width: "37%" },
  colCode: { width: "23%" },
  colPrice: { width: "22%" },
  colMoq: { width: "18%" },

  // ---- Remarks.
  remarkLine: { fontSize: 11, marginBottom: 1.5 * 2.835 },
  remarkBold: { fontFamily: "Helvetica-Bold" },

  // ---- Signature. marginTop:'auto' pushes the block to the bottom of
  // the Page's flex column — so on short quotations it sits anchored
  // at the bottom-left instead of floating just below the remarks.
  // 6mm of bottom padding keeps it clear of the page margin edge.
  sigBlock: {
    marginTop: "auto",
    paddingBottom: 6 * 2.835,
  },
  sigLine:  { fontSize: 10, marginBottom: 1 },
  sigName:  { fontSize: 10, fontFamily: "Helvetica-Bold", marginTop: 6, marginBottom: 1 },
});


/** Top band of the page: NP logo on the left, address center, BSI logo right. */
function Header({ origin }: { origin: string }) {
  return (
    <View style={styles.header}>
      <Image style={styles.logoNP} src={`${origin}/np_logo.jpg`} />
      <View style={styles.addressBlock}>
        <Text style={styles.addrLineBold}>{COMPANY_ADDRESS_LINES[0]}</Text>
        {COMPANY_ADDRESS_LINES.slice(1).map((ln) => (
          <Text style={styles.addrLine} key={ln}>{ln}</Text>
        ))}
      </View>
      <Image style={styles.logoBSI} src={`${origin}/bsi.jpg`} />
    </View>
  );
}


export type QuotePDFProps = {
  data: QuoteData;
  /** Origin used to resolve /np_logo.jpg etc. — pass window.location.origin
   *  on the client so the same component works locally and in production. */
  origin: string;
};


export function QuotePDF({ data, origin }: QuotePDFProps) {
  const sales = data.salesPerson && SALES_PEOPLE[data.salesPerson]
    ? data.salesPerson
    : "Jay";
  const sigInfo = SALES_PEOPLE[sales];

  return (
    <Document
      title={`Quotation — ${data.companyName || "NP Foods"}`}
      author="N. P. Foods (Singapore) Pte Ltd"
    >
      <Page size="A4" style={styles.page}>
        <Header origin={origin} />

        <Text style={styles.date}>{todayLabel()}</Text>

        {/* Customer block */}
        <View style={styles.customerBlock}>
          {data.companyName ? (
            <Text style={styles.custCompany}>{data.companyName}</Text>
          ) : null}
          {data.customerAddress
            .split(/\r?\n/)
            .map((ln) => ln.trim())
            .filter(Boolean)
            .map((ln, i) => (
              <Text style={styles.custLine} key={`addr-${i}`}>{ln}</Text>
            ))}
        </View>

        {data.customerName ? (
          <Text style={styles.greeting}>Dear {data.customerName},</Text>
        ) : null}

        {data.quotationTitle ? (
          <Text style={styles.title}>{data.quotationTitle}</Text>
        ) : null}

        {/* Standard greeting + optional extra comment lines */}
        <View style={styles.comment}>
          <Text style={styles.body}>{DEFAULT_LEAD_COMMENT}</Text>
          {data.extraComment
            .split(/\r?\n/)
            .map((ln) => ln.trim())
            .filter(Boolean)
            .map((ln, i) => (
              <Text style={styles.body} key={`xc-${i}`}>{ln}</Text>
            ))}
        </View>

        {/* Product table */}
        {data.products.length > 0 ? (
          <View style={styles.table}>
            <View style={styles.tableRow}>
              <Text style={[styles.tableHeaderCell, styles.colName]}>Product Name</Text>
              <Text style={[styles.tableHeaderCell, styles.colCode]}>Product Code</Text>
              <Text style={[styles.tableHeaderCell, styles.colPrice]}>Price</Text>
              <Text style={[styles.tableHeaderCell, styles.colMoq]}>MOQ</Text>
            </View>
            {data.products.map((p, i) => (
              <View style={styles.tableRow} key={`p-${i}`}>
                <Text style={[styles.tableCell, styles.colName]}>{p.name}</Text>
                <Text style={[styles.tableCell, styles.colCode]}>{p.code}</Text>
                <Text style={[styles.tableCell, styles.colPrice]}>{priceLabel(p)}</Text>
                <Text style={[styles.tableCell, styles.colMoq]}>{p.moq}</Text>
              </View>
            ))}
          </View>
        ) : null}

        {/* Remark section.
            Packaging Size: for the 20Kg / 25Kg carton options we
            spell out the full carton dimensions + liner spec the
            customer expects on a quotation. Other packaging
            choices (bottle, barrel, pack) show the dropdown value
            as-is. */}
        {data.packagingSize ? (
          (() => {
            const isCartonBox =
              data.packagingSize === "20 KG / BOX" ||
              data.packagingSize === "25 KG / BOX";
            if (isCartonBox) {
              return (
                <Text style={styles.remarkLine}>
                  <Text style={styles.remarkBold}>Packaging Size</Text>
                  {" – 20/25Kg Carton Box (380mm x 290mm x 350mm <H>) "}
                  with one layers of Polyethylene
                </Text>
              );
            }
            return (
              <Text style={styles.remarkLine}>
                <Text style={styles.remarkBold}>Packaging Size: </Text>
                {data.packagingSize}
              </Text>
            );
          })()
        ) : null}
        <Text style={styles.remarkLine}>
          <Text style={styles.remarkBold}>Order Lead time</Text>
          {" "}– approx. 4 - 6 weeks upon receiving of Purchase Order
        </Text>
        {data.paymentTerm ? (
          <Text style={styles.remarkLine}>
            <Text style={styles.remarkBold}>Payment term: </Text>
            {data.paymentTerm}
          </Text>
        ) : null}
        <Text style={styles.remarkLine}>
          <Text style={styles.remarkBold}>Shipping Incoterms: </Text>
          <Text style={styles.remarkBold}>{data.incoterm || "CFR"}</Text>
          {" to "}
          <Text style={styles.remarkBold}>{data.port || "the destination port"}</Text>.
        </Text>
        <Text style={styles.remarkLine}>
          We look forward to your favorable reply. Please do not hesitate to contact any of us if you have further inquiries.
        </Text>
        {data.validityDate ? (
          <Text style={styles.remarkLine}>
            <Text style={styles.remarkBold}>Quotation validity: </Text>
            {data.validityDate}
          </Text>
        ) : null}

        {/* Signature */}
        <View style={styles.sigBlock}>
          <Text style={styles.sigLine}>Your Faithfully,</Text>
          <Text style={styles.sigName}>{sigInfo.fullName}</Text>
          <Text style={styles.sigLine}>N. P. Foods (Singapore) Pte Ltd</Text>
          <Text style={styles.sigLine}>Hp: {sigInfo.hp}</Text>
          <Text style={styles.sigLine}>Tel: +65 6756 2777</Text>
          <Text style={styles.sigLine}>Fax: +65 6756 2555</Text>
        </View>
      </Page>
    </Document>
  );
}
