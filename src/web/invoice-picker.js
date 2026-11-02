export function createInvoicePicker(parent,orders){
 const wrap=document.createElement('div');wrap.className='invoice-picker';
 const label=document.createElement('label');label.textContent='Search imported invoices';
 const search=document.createElement('input');search.type='search';search.autocomplete='off';search.placeholder='Customer name or invoice number';label.append(search);
 const count=document.createElement('p');count.className='invoice-picker-count';count.setAttribute('role','status');
 const list=document.createElement('div');list.className='invoice-picker-list';list.setAttribute('role','radiogroup');list.setAttribute('aria-label','Imported invoice');
 const rows=orders.map(order=>{const row=document.createElement('label');row.className='invoice-picker-row';const radio=document.createElement('input');radio.type='radio';radio.name='orderId';radio.value=order.id;const text=document.createElement('span');text.textContent=order.customer+' · #'+order.invoice;row.append(radio,text);list.append(row);return {row,radio,search:(order.customer+' '+order.invoice).normalize('NFKD').replace(/[\u0300-\u036f]/g,'').toLowerCase()};});
 function filter(){const value=search.value.normalize('NFKD').replace(/[\u0300-\u036f]/g,'').toLowerCase().trim();let visible=0;for(const item of rows){item.row.hidden=!item.search.includes(value);if(item.row.hidden)item.radio.checked=false;else visible++;}count.textContent=visible?visible+' of '+orders.length+' invoices':'No invoices match this search';}
 search.oninput=filter;wrap.append(label,count,list);parent.append(wrap);filter();
 return {get value(){return rows.find(item=>item.radio.checked)?.radio.value||'';}};
}
