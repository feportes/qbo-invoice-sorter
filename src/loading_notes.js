// Physical loading notes, not receipts. Unknown quantities remain unknown.
export const loadingNotes = [];
function add(container, side, fromBack, product, quantity = null, unit = '', note = '') {
  loadingNotes.push({ container, side, fromBack, product, quantity, unit, note });
}
for (let p=1;p<=5;p++) add(2,'L',p,'D Raw Acai Sorbet',140,'tubs');
for (let p=1;p<=3;p++) add(2,'R',p,'D Raw Acai Sorbet',140,'tubs');
add(2,'R',4,'D Raw Acai Sorbet',120,'tubs');
for (let p=1;p<=7;p++) add(5,'L',p,'Acai bricks');
add(5,'L',8,'Acai bricks',54,'cases','Short 2 cases');
for (let p=1;p<=4;p++) add(5,'R',p,'18 kg acai tubs');
add(5,'R',5,'Oakberry acai'); add(5,'R',6,'Oakberry acai');
add(5,'R',7,'Brazilian Blend');
add(5,'R',8,'Passion fruit sorbet',110,'tubs','Partial pallet');
add(5,'R',9,'18 kg unsweetened acai tubs');
add(6,'L',1,'IQF unsweetened',103,'cases','Partial pallet');
add(6,'L',2,'Unsweetened acai packs');
add(6,'R',1,'Sweetened acai packs'); add(6,'R',2,'Sweetened acai packs');
add(6,'R',3,'IQF sweetened'); add(6,'R',4,'Passion fruit sorbet');
for(let p=5;p<=7;p++) add(6,'R',p,'Acai bricks');
add(6,'R',8,'Alohana');
